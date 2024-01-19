package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/fanyang89/zoodb-cli/cmd"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = func(err error) interface{} {
		return fmt.Sprintf("\n%+v", err)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339Nano,
		PartsOrder: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			zerolog.CallerFieldName,
			zerolog.MessageFieldName,
		},
		FieldsExclude: []string{
			zerolog.ErrorStackFieldName,
		},
		FormatExtra: func(m map[string]interface{}, buffer *bytes.Buffer) error {
			s, ok := m["stack"]
			if ok {
				_, err := buffer.WriteString(s.(string))
				return err
			}
			return nil
		},
	})

	app := cmd.NewApp()
	err := app.Run(os.Args)
	if err != nil {
		log.Error().Err(err).Msg("App run failed")
	}
}
