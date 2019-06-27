package gcp

import (
	"context"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/storage/testutils"
)

const (
	proj, instance = "proj", "instance"
)

type fixture struct {
	gcssrv *fakestorage.Server

	name string
}

func (f *fixture) Name() string {
	return f.name
}

func (f *fixture) Clients() (store configs.ConfigStore, err error) {
	f.gcssrv = fakestorage.NewServer(nil)
	f.gcssrv.CreateBucket("configdb")

	return NewGCSConfigClient(context.Background(), GCSConfig{
		BucketName: "configdb",
	})
}

func (f *fixture) Teardown() error {
	f.gcssrv.Stop()
	return nil
}

// Fixtures for unit testing GCP storage.
var Fixtures = func() []testutils.Fixture {
	fixtures := []testutils.Fixture{
		&fixture{name: "gcs"},
	}
	return fixtures
}()
