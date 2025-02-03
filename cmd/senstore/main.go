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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/paraopsde/go-x/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/thz/senstore/pkg/db"
	"github.com/thz/senstore/pkg/kafka"
	"github.com/thz/senstore/pkg/scrape"
)

type runOpts struct {
	scrapeAddress         string
	postgresColumn        string
	postgresConnectString string

	kafkaBootstrap string
	kafkaTopic     string
	kafkaSAKey     string
	kafkaSASeret   string
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "senstore",
		Short: "daemon to push sensor data to a data sync (postgres / kafka)",
		Run: func(cmd *cobra.Command, args []string) {
			opts := runOpts{
				scrapeAddress:         os.Getenv("SCRAPE_ADDRESS"),
				postgresColumn:        os.Getenv("POSTGRES_COLUMN"),
				postgresConnectString: os.Getenv("POSTGRES_CONNECT_STRING"),

				kafkaBootstrap: os.Getenv("KAFKA_BOOTSTRAP"),
				kafkaTopic:     os.Getenv("KAFKA_TOPIC"),
				kafkaSAKey:     os.Getenv("KAFKA_SA_KEY"),
				kafkaSASeret:   os.Getenv("KAFKA_SA_SECRET"),
			}

			if scrapeAddress, _ := cmd.Flags().GetString("scrape-address"); scrapeAddress != "" {
				opts.scrapeAddress = scrapeAddress
			}

			if postgresColumnName, _ := cmd.Flags().GetString("postgres-column"); postgresColumnName != "" {
				opts.postgresColumn = postgresColumnName
			}

			if postgresConnectString, _ := cmd.Flags().GetString("postgres-connect-string"); postgresConnectString != "" {
				opts.postgresConnectString = postgresConnectString
			}

			ctx := context.Background()
			log := util.NewLogger()
			ctx = util.CtxWithLog(ctx, log)

			if err := run(ctx, opts); err != nil {
				log.Error("failed to execute command", zap.Error(err))
				os.Exit(1)
			}
		},
	}
	rootCmd.Flags().StringP("scrape-address", "s", "http://127.0.0.1:9000/metrics", "address to scrape readings")
	rootCmd.Flags().StringP("postgres-column", "c", "reading", "database column to write readings to")
	rootCmd.Flags().StringP("postgres-connect-string", "p", "", "postgres connection string")
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, opts runOpts) error {
	log := util.CtxLogOrPanic(ctx)
	scraper := scrape.NewScraper(opts.scrapeAddress)

	var (
		dbWriter    *db.Writer
		kafkaWriter *kafka.Writer
	)
	if opts.postgresConnectString != "" {
		dbWriter = db.NewWriter(opts.postgresConnectString)
		if opts.postgresColumn != "" {
			dbWriter.SetReadingColumn(opts.postgresColumn)
		}
		log.Info("using postgres writer",
			zap.String("column", dbWriter.ReadingColumn()))
	}

	if opts.kafkaBootstrap != "" {
		kafkaWriter = kafka.NewWriter(kafka.Opts{
			Topic:     opts.kafkaTopic,
			SAKey:     opts.kafkaSAKey,
			SASecret:  opts.kafkaSASeret,
			Bootstrap: opts.kafkaBootstrap,
		})
		log.Info("using kafka writer",
			zap.String("topic", opts.kafkaTopic),
			zap.String("bootstrap", opts.kafkaBootstrap),
			zap.String("sa_key", opts.kafkaSAKey),
		)
	}
	ticker := time.NewTicker(15 * time.Second)

	timestamp := time.Now()
	for {
		data, err := scraper.Scrape(ctx)
		if err != nil {
			log.Error("failed to scrape", zap.Error(err))
			return fmt.Errorf("failed to scrape: %w", err)
		}

		if dbWriter != nil {
			err = dbWriter.Write(ctx, timestamp, data)
			if err != nil {
				log.Error("failed to write to db", zap.Error(err))
				return fmt.Errorf("failed to write to db: %w", err)
			}
			log.Info("wrote to db",
				zap.String("column", dbWriter.ReadingColumn()),
				zap.Time("timestamp", timestamp),
				zap.Any("data", data),
			)
		}

		if kafkaWriter != nil {
			jsonBytesReadings, err := json.Marshal(data)
			if err != nil {
				log.Error("failed to marshal data", zap.Error(err))
				return fmt.Errorf("failed to marshal data: %w", err)
			}

			err = kafkaWriter.Produce(timestamp, jsonBytesReadings)
			if err != nil {
				log.Error("failed to write to kafka", zap.Error(err))
				return fmt.Errorf("failed to write to kafka: %w", err)
			}
			log.Info("produced to kafka",
				zap.String("topic", opts.kafkaTopic),
				zap.String("data", string(jsonBytesReadings)),
				zap.Time("timestamp", timestamp),
			)
		}

		select {
		case <-ctx.Done():
			return errors.New("context done")
		case timestamp = <-ticker.C:
		}
	}
}
