package gcp

import (
	alertStore "github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/ruler/store"
	"github.com/cortexproject/cortex/pkg/storage/testutils"
	"github.com/fsouza/fake-gcs-server/fakestorage"
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

func (f *fixture) Clients() (alertStore.AlertStore, store.RuleStore, error) {
	f.gcssrv = fakestorage.NewServer(nil)
	f.gcssrv.CreateBucket("configdb")
	cli := newGCSClient(GCSConfig{
		BucketName: "configdb",
	}, f.gcssrv.Client())

	return cli, cli, nil
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
