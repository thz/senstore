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

package scrape

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/paraopsde/go-x/pkg/util"
	"go.uber.org/zap"
)

type Scraper struct {
	endpoint string
}

func NewScraper(endpoint string) *Scraper {
	return &Scraper{
		endpoint: endpoint,
	}
}

func (s *Scraper) Scrape(ctx context.Context) (map[string]float32, error) {
	log := util.CtxLogOrPanic(ctx)

	data := make(map[string]float32)

	// make http request to endpoint
	c := http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", s.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// parse response
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "go_") {
			continue
		}

		if strings.HasPrefix(line, "promhttp_") {
			continue
		}

		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			log.Warn("invalid line", zap.String("line", line))
			continue
		}

		value, err := strconv.ParseFloat(parts[1], 32)
		if err != nil {
			log.Warn("failed to parse value", zap.String("value", parts[1]))
			continue
		}
		data[parts[0]] = float32(value)
	}

	return data, nil
}
