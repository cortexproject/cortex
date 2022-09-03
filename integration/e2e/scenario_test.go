//go:build requires_docker
// +build requires_docker

package e2e_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/s3"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
)

const bktName = "cheesecake"

func spinup(t *testing.T, networkName string) (*e2e.Scenario, *e2e.HTTPService, *e2e.HTTPService) {
	s, err := e2e.NewScenario(networkName)
	assert.NoError(t, err)

	m1 := e2edb.NewMinio(9000, bktName)
	m2 := e2edb.NewMinio(9001, bktName)

	closePlease := true
	defer func() {
		if closePlease {
			// You're welcome.
			s.Close()
		}
	}()
	require.NoError(t, s.StartAndWaitReady(m1, m2))
	require.Error(t, s.Start(m1))
	require.Error(t, s.Start(e2edb.NewMinio(9000, "cheescake")))

	closePlease = false
	return s, m1, m2
}

func testMinioWorking(t *testing.T, m *e2e.HTTPService) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	b, err := yaml.Marshal(s3.Config{
		Endpoint:  m.HTTPEndpoint(),
		Bucket:    bktName,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Insecure:  true, // WARNING: Our secret cheesecake recipes might leak.
	})
	require.NoError(t, err)

	bkt, err := s3.NewBucket(log.NewNopLogger(), b, "test")
	require.NoError(t, err)

	require.NoError(t, bkt.Upload(ctx, "recipe", bytes.NewReader([]byte("Just go to Pastry Shop and buy."))))
	require.NoError(t, bkt.Upload(ctx, "mom/recipe", bytes.NewReader([]byte("https://www.bbcgoodfood.com/recipes/strawberry-cheesecake-4-easy-steps"))))

	r, err := bkt.Get(ctx, "recipe")
	require.NoError(t, err)
	b, err = ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "Just go to Pastry Shop and buy.", string(b))

	r, err = bkt.Get(ctx, "mom/recipe")
	require.NoError(t, err)
	b, err = ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "https://www.bbcgoodfood.com/recipes/strawberry-cheesecake-4-easy-steps", string(b))
}

func TestScenario(t *testing.T) {
	t.Parallel()

	s, m1, m2 := spinup(t, "e2e-scenario-test")
	defer s.Close()

	t.Run("minio is working", func(t *testing.T) {
		testMinioWorking(t, m1)
		testMinioWorking(t, m2)
	})

	t.Run("concurrent nested scenario 1 is working just fine as well", func(t *testing.T) {
		t.Parallel()

		s, m1, m2 := spinup(t, "e2e-scenario-test1")
		defer s.Close()

		testMinioWorking(t, m1)
		testMinioWorking(t, m2)
	})
	t.Run("concurrent nested scenario 2 is working just fine as well", func(t *testing.T) {
		t.Parallel()

		s, m1, m2 := spinup(t, "e2e-scenario-test2")
		defer s.Close()

		testMinioWorking(t, m1)
		testMinioWorking(t, m2)
	})

	require.NoError(t, s.Stop(m1))

	// Expect m1 not working.
	b, err := yaml.Marshal(s3.Config{
		Endpoint:  m1.Name(),
		Bucket:    "cheescake",
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	})
	require.NoError(t, err)
	bkt, err := s3.NewBucket(log.NewNopLogger(), b, "test")
	require.NoError(t, err)

	_, err = bkt.Get(context.Background(), "recipe")
	require.Error(t, err)

	testMinioWorking(t, m2)

	require.Error(t, s.Stop(m1))
	// Should be noop.
	require.NoError(t, m1.Stop())
	// I can run closes as many times I want.
	s.Close()
	s.Close()
	s.Close()

	// Expect m2 not working.
	b, err = yaml.Marshal(s3.Config{
		Endpoint:  m2.Name(),
		Bucket:    "cheescake",
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	})
	require.NoError(t, err)
	bkt, err = s3.NewBucket(log.NewNopLogger(), b, "test")
	require.NoError(t, err)

	_, err = bkt.Get(context.Background(), "recipe")
	require.Error(t, err)
}
