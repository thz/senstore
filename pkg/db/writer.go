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

package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/paraopsde/go-x/pkg/util"
	"go.uber.org/zap"
)

type Writer struct {
	db       *sql.DB
	sensors  map[string]int32
	prepared bool
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Prepare(ctx context.Context) error {
	log := util.CtxLogOrPanic(ctx)
	w.maybeConnect(ctx)

	w.sensors = make(map[string]int32)
	rows, err := w.db.Query(`SELECT id,name FROM sensors`)
	if err != nil {
		return fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id   int32
			name string
		)
		err = rows.Scan(&id, &name)
		if err != nil {
			return fmt.Errorf("failed to scan: %w", err)
		}
		w.sensors[name] = id
	}

	w.prepared = true
	log.Info("sensors", zap.Any("sensors", w.sensors))
	return nil
}

func (w *Writer) Write(ctx context.Context, ts time.Time, data map[string]float32) error {
	log := util.CtxLogOrPanic(ctx)

	err := w.maybeConnect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

	if !w.prepared {
		return fmt.Errorf("writer not prepared")
	}

	txn, err := w.db.Begin()
	if err != nil {
		log.Error("failed to begin transaction", zap.Error(err))
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := w.db.Prepare(`INSERT into sensor_readings (ts, sensor, data) VALUES ($1, $2, $3)`)
	if err != nil {
		log.Error("failed to prepare statement", zap.Error(err))
		return fmt.Errorf("failed to prepare: %w", err)
	}

	count := 0
	for key, value := range data {
		sensorID, ok := w.sensors[key]
		if !ok {
			log.Warn("unknown sensor", zap.String("sensor", key))
			continue
		}

		_, err = stmt.Exec(ts.UTC(), sensorID, value)
		if err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
		count += 1
	}

	err = txn.Commit()
	if err != nil {
		log.Error("failed to commit transaction", zap.Error(err))
		return fmt.Errorf("failed to commit: %w", err)
	}
	log.Info("committed transaction", zap.String("ts", ts.Format(time.RFC3339)), zap.Int("count", count))

	return nil
}

func (w *Writer) maybeConnect(ctx context.Context) error {
	if w.db != nil {
		return nil
	}
	log := util.CtxLogOrPanic(ctx)
	log.Info("connecting to db")
	connStr := os.Getenv("CONN")
	if connStr == "" {
		return fmt.Errorf("CONN env var not set")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

	w.db = db

	if err = w.Prepare(ctx); err != nil {
		return fmt.Errorf("failed to prepare: %w", err)
	}

	return nil
}
