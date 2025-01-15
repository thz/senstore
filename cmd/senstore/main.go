// Copyright 2025 Tobias Hintze
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/paraopsde/go-x/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/thz/senstore/pkg/db"
	"github.com/thz/senstore/pkg/scrape"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "senstore",
		Short: "daemon to push sensor data to sql database",
		Run: func(cmd *cobra.Command, args []string) {
			scrapeAddr, _ := cmd.Flags().GetString("scrape-address")
			columnName, _ := cmd.Flags().GetString("column")

			ctx := context.Background()
			log := util.NewLogger()
			ctx = util.CtxWithLog(ctx, log)

			if err := run(ctx, scrapeAddr, columnName); err != nil {
				log.Error("failed to execute command", zap.Error(err))
				os.Exit(1)
			}
		},
	}
	rootCmd.Flags().StringP("scrape-address", "s", "http://127.0.0.1:9000/metrics", "address to scrape readings")
	rootCmd.Flags().StringP("column", "c", "reading", "database column to write readings to")
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, scrapeAddress, columnName string) error {
	log := util.CtxLogOrPanic(ctx)
	scraper := scrape.NewScraper(scrapeAddress)

	dbWriter := db.NewWriter()
	if columnName != "" {
		dbWriter.SetReadingColumn(columnName)
	}
	ticker := time.NewTicker(15 * time.Second)

	timestamp := time.Now()
	for {
		data, err := scraper.Scrape(ctx)
		if err != nil {
			log.Error("failed to scrape", zap.Error(err))
			return fmt.Errorf("failed to scrape: %w", err)
		}
		err = dbWriter.Write(ctx, timestamp, data)
		if err != nil {
			log.Error("failed to write to db", zap.Error(err))
			return fmt.Errorf("failed to write to db: %w", err)
		}

		select {
		case <-ctx.Done():
			return errors.New("context done")
		case timestamp = <-ticker.C:
		}
	}
}
