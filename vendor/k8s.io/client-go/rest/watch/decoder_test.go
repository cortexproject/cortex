/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package versioned_test

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	restclientwatch "k8s.io/client-go/rest/watch"

	_ "k8s.io/client-go/pkg/api/install"
)

func TestDecoder(t *testing.T) {
	table := []watch.EventType{watch.Added, watch.Deleted, watch.Modified, watch.Error}

	for _, eventType := range table {
		out, in := io.Pipe()
		codec := api.Codecs.LegacyCodec(v1.SchemeGroupVersion)
		decoder := restclientwatch.NewDecoder(streaming.NewDecoder(out, codec), codec)

		expect := &api.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
		encoder := json.NewEncoder(in)
		go func() {
			data, err := runtime.Encode(api.Codecs.LegacyCodec(v1.SchemeGroupVersion), expect)
			if err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			event := metav1.WatchEvent{
				Type:   string(eventType),
				Object: runtime.RawExtension{Raw: json.RawMessage(data)},
			}
			if err := encoder.Encode(&event); err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			in.Close()
		}()

		done := make(chan struct{})
		go func() {
			action, got, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if e, a := eventType, action; e != a {
				t.Errorf("Expected %v, got %v", e, a)
			}
			if e, a := expect, got; !apiequality.Semantic.DeepDerivative(e, a) {
				t.Errorf("Expected %v, got %v", e, a)
			}
			t.Logf("Exited read")
			close(done)
		}()
		<-done

		done = make(chan struct{})
		go func() {
			_, _, err := decoder.Decode()
			if err == nil {
				t.Errorf("Unexpected nil error")
			}
			close(done)
		}()
		<-done

		decoder.Close()
	}
}

func TestDecoder_SourceClose(t *testing.T) {
	out, in := io.Pipe()
	codec := api.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	decoder := restclientwatch.NewDecoder(streaming.NewDecoder(out, codec), codec)

	done := make(chan struct{})

	go func() {
		_, _, err := decoder.Decode()
		if err == nil {
			t.Errorf("Unexpected nil error")
		}
		close(done)
	}()

	in.Close()

	select {
	case <-done:
		break
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("Timeout")
	}
}
