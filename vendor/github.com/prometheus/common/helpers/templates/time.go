// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package templates

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

func convertToFloat(i interface{}) (float64, error) {
	switch v := i.(type) {
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case int:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case time.Duration:
		return v.Seconds(), nil
	default:
		return 0, fmt.Errorf("can't convert %T to float", v)
	}
}

func HumanizeDuration(i interface{}) (string, error) {
	v, err := convertToFloat(i)
	if err != nil {
		return "", err
	}

	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Sprintf("%.4g", v), nil
	}
	if v == 0 {
		return fmt.Sprintf("%.4gs", v), nil
	}
	if math.Abs(v) >= 1 {
		sign := ""
		if v < 0 {
			sign = "-"
			v = -v
		}
		duration := int64(v)
		seconds := duration % 60
		minutes := (duration / 60) % 60
		hours := (duration / 60 / 60) % 24
		days := duration / 60 / 60 / 24
		// For days to minutes, we display seconds as an integer.
		if days != 0 {
			return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds), nil
		}
		if hours != 0 {
			return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds), nil
		}
		if minutes != 0 {
			return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds), nil
		}
		// For seconds, we display 4 significant digits.
		return fmt.Sprintf("%s%.4gs", sign, v), nil
	}
	prefix := ""
	for _, p := range []string{"m", "u", "n", "p", "f", "a", "z", "y"} {
		if math.Abs(v) >= 1 {
			break
		}
		prefix = p
		v *= 1000
	}
	return fmt.Sprintf("%.4g%ss", v, prefix), nil
}
