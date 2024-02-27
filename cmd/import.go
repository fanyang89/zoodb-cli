package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-zookeeper/zk"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"

	"github.com/fanyang89/zoodb-cli/zoodb"
)

func DeleteAll(conn *zk.Conn, path string) error {
	children, _, err := conn.Children(path)
	if err != nil {
		return errors.Wrapf(err, "get children failed, path: %v", path)
	}

	if len(children) == 0 {
		err = conn.Delete(path, -1)
		if err != nil {
			return errors.Wrapf(err, "delete %v failed", path)
		}
	} else {
		for _, child := range children {
			err = DeleteAll(conn, filepath.Join(path, child))
			if err != nil {
				return errors.Wrapf(err, "delete all %v failed", path)
			}
		}
		err = conn.Delete(path, -1)
		if err != nil {
			return errors.Wrapf(err, "delete self %v failed", path)
		}
	}

	return nil
}

var cmdImport = &cli.Command{
	Name: "import",
	Flags: []cli.Flag{
		fileFlag, prefixFlag, hostsFlag, sessionTimeoutFlag, overwriteFlag, clearFlag,
	},
	Action: func(c *cli.Context) error {
		file := c.String("file")
		fh, err := os.Open(file)
		if err != nil {
			return errors.Wrap(err, "open failed")
		}
		defer func() { _ = fh.Close() }()

		s := bufio.NewScanner(fh)
		db, err := zoodb.NewZooDb(s)
		if err != nil {
			return errors.Wrap(err, "parse zoo db failed")
		}

		log.Info().Int("sessions", len(db.Sessions)).
			Int("znodes", len(db.Znodes)).
			Msg("parse completed")

		// sort znodes
		sort.SliceStable(db.Znodes, func(i, j int) bool {
			return getDepth(db.Znodes[i].Path) < getDepth(db.Znodes[j].Path)
		})

		conn, events, err := zk.Connect(
			strings.FieldsFunc(c.String("hosts"), func(r rune) bool { return r == ',' }),
			c.Duration("session-timeout"), zk.WithLogger(&ZkZeroLogger{}))
		if err != nil {
			return errors.Wrap(err, "connect failed")
		}

		ctx, cancel := context.WithCancel(context.TODO())
		go func(ctx context.Context, events <-chan zk.Event) {
			for {
				select {
				case e := <-events:
					log.Info().Msgf("New event arrived, %+v", e)
				case <-ctx.Done():
					log.Info().Msg("Event watcher exiting")
					return
				}
			}
		}(ctx, events)

		defer func() {
			cancel()
			conn.Close()
		}()

		prefix := c.String("prefix")
		if c.Bool("clear") {
			err = DeleteAll(conn, prefix)
			if err != nil {
				return err
			}
		}

		bar := progressbar.Default(int64(len(db.Znodes)))
		for _, n := range db.Znodes {
			p := filepath.Join(prefix, n.Path)
			_, err = conn.Create(p, n.Data, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					if c.Bool("overwrite") {
						_, err = conn.Set(p, n.Data, -1)
						if err != nil {
							bar.Describe(fmt.Sprintf("set %s failed, err: %v", p, err))
						}
					}
				} else {
					bar.Describe(fmt.Sprintf("create %s failed, err: %v", p, err))
				}
			}
			_ = bar.Add(1)
		}
		_ = bar.Finish()

		_, err = conn.Sync(prefix)
		if err != nil {
			log.Warn().Err(err).Msg("sync failed")
		}

		bar.Describe("Import completed")
		return nil
	},
}
