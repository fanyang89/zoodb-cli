package cmd

import "github.com/rs/zerolog/log"

type ZkZeroLogger struct{}

func (z *ZkZeroLogger) Printf(s string, i ...interface{}) {
	log.Info().Msgf(s, i...)
}
