package gcp

import (
	"github.com/cortexproject/cortex/pkg/storage/alerts"
	"github.com/cortexproject/cortex/pkg/storage/rules"
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

func (f *fixture) Clients() (alerts.AlertStore, rules.RuleStore, error) {
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
