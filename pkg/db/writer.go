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
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/paraopsde/go-x/pkg/util"
	"go.uber.org/zap"
)

type Writer struct {
	db            *sql.DB
	sensors       map[string]int32
	prepared      bool
	readingColumn string
	connStr       string
}

func NewWriter(connStr string) *Writer {
	return &Writer{
		readingColumn: "reading",
		connStr:       connStr,
	}
}

func (w *Writer) SetReadingColumn(readingColumn string) {
	w.readingColumn = readingColumn
}

func (w *Writer) ReadingColumn() string {
	return w.readingColumn
}

func (w *Writer) Prepare(ctx context.Context) error {
	log := util.CtxLogOrPanic(ctx)

	if err := w.maybeConnect(ctx); err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

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

func (w *Writer) Write(ctx context.Context, ts time.Time, data map[string]int64) error {
	log := util.CtxLogOrPanic(ctx)

	err := w.maybeConnect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

	if !w.prepared {
		return errors.New("writer not prepared")
	}

	txn, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := w.db.Prepare(fmt.Sprintf("INSERT into sensor_readings (ts, sensor, %s) VALUES ($1, $2, $3)", w.readingColumn))
	if err != nil {
		return fmt.Errorf("failed to prepare: %w", err)
	}
	defer stmt.Close()

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
	log.Info("connecting to db", zap.String("conn", w.connStr))
	if w.connStr == "" {
		return errors.New("postgres connection string not set")
	}

	db, err := sql.Open("postgres", w.connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

	w.db = db

	if err = w.Prepare(ctx); err != nil {
		return fmt.Errorf("failed to prepare: %w", err)
	}

	return nil
}
