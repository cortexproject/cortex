package kuberesolver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/naming"
)

func TestParseTarget(t *testing.T) {
	// kubernetes://otsimo-watch:portname
	// kubernetes://otsimo-watch:45638
	// dns://10.0.1.4:2341
	// 10.0.1.4:2341
	tests := []struct {
		target string
		result targetInfo
		err    error
	}{
		{
			target: "kubernetes://otsimo-watch:portname",
			result: targetInfo{
				urlType:           TargetTypeKubernetes,
				target:            "otsimo-watch",
				port:              "portname",
				resolveByPortName: true,
				useFirstPort:      false,
			},
		},
		{
			target: "kubernetes://service-name:5638",
			result: targetInfo{
				urlType:           TargetTypeKubernetes,
				target:            "service-name",
				port:              "5638",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
		{
			target: "kubernetes://service-name",
			result: targetInfo{
				urlType:           TargetTypeKubernetes,
				target:            "service-name",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      true,
			},
		},
		{
			target: "dns://10.0.1.4:2341",
			result: targetInfo{
				urlType:           TargetTypeDNS,
				target:            "10.0.1.4:2341",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
		{
			target: "10.0.1.4:2341",
			result: targetInfo{
				urlType:           TargetTypeDNS,
				target:            "10.0.1.4:2341",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
		{
			target: "services.otsimo.com:2315",
			result: targetInfo{
				urlType:           TargetTypeDNS,
				target:            "services.otsimo.com:2315",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
		{
			target: "services.otsimo.com",
			result: targetInfo{
				urlType:           TargetTypeDNS,
				target:            "services.otsimo.com",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
		{
			target: "tcp://10.0.1.4:2341",
			result: targetInfo{
				urlType:           TargetTypeDNS,
				target:            "tcp://10.0.1.4:2341",
				port:              "",
				resolveByPortName: false,
				useFirstPort:      false,
			},
		},
	}

	for i, test := range tests {
		ti, err := parseTarget(test.target)
		if test.err != nil {
			if !reflect.DeepEqual(err, test.err) {
				t.Fatal(fmt.Errorf("case %d failed, errors are not equal", i))
			}
			continue
		} else {
			if err != nil {
				t.Fatal(fmt.Errorf("case %d failed, errors must be nil but found, %v", i, err))
			}
		}
		if !reflect.DeepEqual(ti, test.result) {
			t.Fatal(fmt.Errorf("case %d failed, want='%+v' got='%+v'", i, test.result, ti))
		}
	}
}

func TestConnection(t *testing.T) {
	caCrtFile := os.Getenv("TEST_K8S_CA_CRT")
	clientCrtFile := os.Getenv("TEST_K8S_CLIENT_CRT")
	clientKeyFile := os.Getenv("TEST_K8S_CLIENT_KEY")
	apiHost := os.Getenv("TEST_K8S_API_HOST")
	service := os.Getenv("TEST_K8S_SERVICE")
	servicePort := os.Getenv("TEST_K8S_SERVICE_PORT")
	if len(caCrtFile) == 0 || len(clientCrtFile) == 0 || len(clientKeyFile) == 0 || len(apiHost) == 0 {
		t.Fatal("")
	}
	ca, err := ioutil.ReadFile(caCrtFile)
	if err != nil {
		t.Fatal(err)
	}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(ca)
	cert, err := tls.LoadX509KeyPair(clientCrtFile, clientKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion:   tls.VersionTLS10,
		RootCAs:      certPool,
		Certificates: []tls.Certificate{cert},
	}}
	client := &k8sClient{
		host:       apiHost,
		httpClient: &http.Client{Transport: transport},
	}
	ti := targetInfo{
		urlType:           TargetTypeKubernetes,
		target:            service,
		port:              servicePort,
		resolveByPortName: false,
		useFirstPort:      false,
	}
	kr := newResolver(client, "default", ti)
	w, err := kr.Resolve(ti.target)
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan struct {
		up []*naming.Update
	})
	errChan := make(chan error)
	go func() {
		for {
			u2, err := w.Next()
			if err != nil {
				errChan <- err
			} else {
				res <- struct {
					up []*naming.Update
				}{
					up: u2,
				}
			}
		}
	}()
	for {
		select {
		case rrr := <-res:
			for _, r := range rrr.up {
				fmt.Printf("u1: %s %d\n", r.Addr, r.Op)
			}
		case <-time.After(time.Second * 180):
			w.Close()
			t.Fatal("time out")
		case err := <-errChan:
			t.Fatal(err)
		}
	}
}
