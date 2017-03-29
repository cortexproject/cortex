// Definitions in this file were largely taken from
// https://github.com/prometheus/alertmanager/blob/master/cmd/alertmanager/main.go
//
// Original copyright notice:
//
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

package alertmanager

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/weaveworks/mesh"
)

func initMesh(addr, hwaddr, nickname, pw string) *mesh.Router {
	host, portStr, err := net.SplitHostPort(addr)

	if err != nil {
		log.Fatalf("mesh address: %s: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("mesh address: %s: %v", addr, err)
	}

	name, err := mesh.PeerNameFromString(hwaddr)
	if err != nil {
		log.Fatalf("invalid hardware address %q: %v", hwaddr, err)
	}

	password := []byte(pw)
	if len(password) == 0 {
		// Empty password is used to disable secure communication. Using a nil
		// password disables encryption in mesh.
		password = nil
	}

	return mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		Password:           password,
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, nickname, mesh.NullOverlay{}, log.New(ioutil.Discard, "", 0))

}

type stringset map[string]struct{}

func (ss stringset) Set(value string) error {
	ss[value] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}

func mustHardwareAddr() string {
	// TODO(fabxc): consider a safe-guard against colliding MAC addresses.
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if s := iface.HardwareAddr.String(); s != "" {
			return s
		}
	}
	panic("no valid network interfaces")
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

type peerDescSlice []mesh.PeerDescription

func (s peerDescSlice) Len() int           { return len(s) }
func (s peerDescSlice) Less(i, j int) bool { return s[i].UID < s[j].UID }
func (s peerDescSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// meshWait returns a function that inspects the current peer state and returns
// a duration of one base timeout for each peer with a higher ID than ourselves.
func meshWait(r GossipRouter, timeout time.Duration) func() time.Duration {
	return func() time.Duration {
		var peers peerDescSlice
		for _, desc := range r.GetPeers().Descriptions() {
			peers = append(peers, desc)
		}
		sort.Sort(peers)

		k := 0
		for _, desc := range peers {
			if desc.Self {
				break
			}
			k++
		}
		// TODO(fabxc): add metric exposing the "position" from AM's own view.
		return time.Duration(k) * timeout
	}
}
