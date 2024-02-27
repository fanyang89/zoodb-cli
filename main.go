package main

import (
	"os"

	"github.com/fanyang89/zerologging/v1"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/fanyang89/zoodb-cli/cmd"
)

func main() {
	zerologging.WithConsoleLog(zerolog.InfoLevel)
	app := cmd.NewApp()
	err := app.Run(os.Args)
	if err != nil {
		log.Error().Err(err).Msg("App run failed")
	}
}
