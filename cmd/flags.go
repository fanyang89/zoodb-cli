package cmd

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v2"
)

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

var pathFlag = &cli.StringFlag{
	Name: "path",
	Action: func(c *cli.Context, s string) error {
		if s == "" {
			return errors.New("path is empty")
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

var clearFlag = &cli.BoolFlag{
	Name:    "clear",
	Aliases: []string{"C"},
	Value:   false,
}
