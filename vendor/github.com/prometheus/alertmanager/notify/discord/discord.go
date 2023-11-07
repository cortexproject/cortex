// Copyright 2021 Prometheus Team
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

package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	commoncfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
)

const (
	colorRed   = 0x992D22
	colorGreen = 0x2ECC71
	colorGrey  = 0x95A5A6
)

// Notifier implements a Notifier for Discord notifications.
type Notifier struct {
	conf       *config.DiscordConfig
	tmpl       *template.Template
	logger     log.Logger
	client     *http.Client
	retrier    *notify.Retrier
	webhookURL *config.SecretURL
}

// New returns a new Discord notifier.
func New(c *config.DiscordConfig, t *template.Template, l log.Logger, httpOpts ...commoncfg.HTTPClientOption) (*Notifier, error) {
	client, err := commoncfg.NewClientFromConfig(*c.HTTPConfig, "discord", httpOpts...)
	if err != nil {
		return nil, err
	}
	n := &Notifier{
		conf:       c,
		tmpl:       t,
		logger:     l,
		client:     client,
		retrier:    &notify.Retrier{},
		webhookURL: c.WebhookURL,
	}
	return n, nil
}

type webhook struct {
	Content string         `json:"content"`
	Embeds  []webhookEmbed `json:"embeds"`
}

type webhookEmbed struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Color       int    `json:"color"`
}

// Notify implements the Notifier interface.
func (n *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	key, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return false, err
	}

	level.Debug(n.logger).Log("incident", key)

	alerts := types.Alerts(as...)
	data := notify.GetTemplateData(ctx, n.tmpl, as, n.logger)
	tmpl := notify.TmplText(n.tmpl, data, &err)
	if err != nil {
		return false, err
	}

	title := tmpl(n.conf.Title)
	if err != nil {
		return false, err
	}
	description := tmpl(n.conf.Message)
	if err != nil {
		return false, err
	}

	color := colorGrey
	if alerts.Status() == model.AlertFiring {
		color = colorRed
	}
	if alerts.Status() == model.AlertResolved {
		color = colorGreen
	}

	w := webhook{
		Embeds: []webhookEmbed{{
			Title:       title,
			Description: description,
			Color:       color,
		}},
	}

	var payload bytes.Buffer
	if err = json.NewEncoder(&payload).Encode(w); err != nil {
		return false, err
	}

	resp, err := notify.PostJSON(ctx, n.client, n.webhookURL.String(), &payload)
	if err != nil {
		return true, notify.RedactURL(err)
	}

	shouldRetry, err := n.retrier.Check(resp.StatusCode, resp.Body)
	if err != nil {
		return shouldRetry, err
	}
	return false, nil
}
