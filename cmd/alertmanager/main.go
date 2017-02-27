// Copyright 2015 Prometheus Team
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

package main

import (
	"flag"
	"log"

	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/alertmanager"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
		}
		alertmanagerConfig alertmanager.MultitenantAlertmanagerConfig
	)
	util.RegisterFlags(&serverConfig, &alertmanagerConfig)
	flag.Parse()

	multiAM, err := alertmanager.NewMultitenantAlertmanager(&alertmanagerConfig)
	if err != nil {
		log.Fatalf("Error initializing MultitenantAlertmanager: %v", err)
	}
	go multiAM.Run()
	defer multiAM.Stop()

	server := server.New(serverConfig)
	server.HTTP.PathPrefix("/api/prom").Handler(multiAM)
	defer server.Stop()
	server.Run()
}
