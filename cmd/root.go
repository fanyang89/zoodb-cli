package cmd

import (
	"github.com/urfave/cli/v2"
)

func NewApp() *cli.App {
	return &cli.App{
		Name:  "zoodb-cli",
		Usage: "ZooKeeper database utils",
		Commands: []*cli.Command{
			cmdImport,
			cmdDu,
		},
	}
}
