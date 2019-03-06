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

	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		alertmanagerConfig alertmanager.MultitenantAlertmanagerConfig
	)
	flagext.RegisterFlags(&serverConfig, &alertmanagerConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	multiAM, err := alertmanager.NewMultitenantAlertmanager(&alertmanagerConfig)
	util.CheckFatal("initializing MultitenantAlertmanager", err)
	go multiAM.Run()
	defer multiAM.Stop()

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	server.HTTP.PathPrefix("/status").Handler(multiAM.GetStatusHandler())
	server.HTTP.PathPrefix("/api/prom").Handler(middleware.AuthenticateUser.Wrap(multiAM))
	server.Run()
}
