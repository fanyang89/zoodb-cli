package cmd

import (
	"context"
	"path"
	"strings"

	"github.com/ahmetb/go-linq"
	"github.com/cockroachdb/errors"
	"github.com/go-zookeeper/zk"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/maps"
)

var cmdDu = &cli.Command{
	Name: "du",
	Flags: []cli.Flag{
		hostsFlag, sessionTimeoutFlag, pathFlag,
	},
	Action: func(c *cli.Context) error {
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

		stats := make(map[string]zk.Stat)
		path := c.String("path")
		err = walk(conn, path, func(path string, s zk.Stat) error {
			stats[path] = s
			return nil
		})
		if err != nil {
			return err
		}

		total := linq.From(maps.Values(stats)).
			SelectT(func(s zk.Stat) int32 { return s.DataLength }).
			SumInts()

		log.Info().Int("count", len(stats)).
			Int64("totalSize", total).Msg("du result")
		return nil
	},
}

func walk(conn *zk.Conn, root string, fn func(path string, s zk.Stat) error) error {
	children, stat, err := conn.Children(root)
	if err != nil {
		return errors.Wrapf(err, "get children for %s failed", root)
	}

	err = fn(root, *stat)
	if err != nil {
		return err
	}

	for _, child := range children {
		err = walk(conn, path.Join(root, child), fn)
		if err != nil {
			return err
		}
	}

	return nil
}
