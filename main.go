package main

import (
	"os"

	"github.com/kvalv/scraper/scraper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
    log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
    zerolog.SetGlobalLevel(zerolog.InfoLevel)
    menyScraper := scraper.MenyScraper{}
    menyScraper.Fetch()
}
