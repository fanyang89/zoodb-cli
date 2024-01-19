package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-zookeeper/zk"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"

	"github.com/fanyang89/zoodb-cli/zoodb"
)

func fileExists(p string) (bool, error) {
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

var fileFlag = &cli.StringFlag{
	Name:    "file",
	Aliases: []string{"f"},
	Action: func(c *cli.Context, s string) error {
		ok, err := fileExists(s)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Newf("file %v not exists", s)
		}
		return nil
	},
}

var prefixFlag = &cli.StringFlag{
	Name:    "prefix",
	Aliases: []string{"p"},
	Action: func(c *cli.Context, s string) error {
		if s == "" {
			return errors.New("prefix is empty")
		}
		return nil
	},
}

var sessionTimeoutFlag = &cli.DurationFlag{
	Name: "session-timeout", Value: 10 * time.Second,
}

var overwriteFlag = &cli.BoolFlag{
	Name:  "overwrite",
	Value: false,
}

var hostsFlag = &cli.StringFlag{
	Name: "hosts",
}

func getDepth(key string) int {
	// / for 1
	// /zookeeper/config for 2
	c := 0
	for _, a := range key {
		if a == '/' {
			c++
		}
	}
	return c
}

type ZkZeroLogger struct{}

func (z *ZkZeroLogger) Printf(s string, i ...interface{}) {
	log.Info().Msgf(s, i...)
}

var cmdImport = &cli.Command{
	Name: "import",
	Flags: []cli.Flag{
		fileFlag, prefixFlag, hostsFlag, sessionTimeoutFlag, overwriteFlag,
	},
	Action: func(c *cli.Context) error {
		file := c.String("file")
		fh, err := os.Open(file)
		if err != nil {
			return errors.Wrap(err, "open failed")
		}
		defer fh.Close()

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
