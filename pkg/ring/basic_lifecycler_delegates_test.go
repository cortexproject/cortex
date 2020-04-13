package ring

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestLeaveOnStoppingDelegate(t *testing.T) {
	onStoppingCalled := false

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()

	testDelegate := &mockDelegate{
		onStopping: func(l *BasicLifecycler) {
			assert.Equal(t, LEAVING, l.GetState())
			onStoppingCalled = true
		},
	}

	leaveDelegate := NewLeaveOnStoppingDelegate(testDelegate, log.NewNopLogger())
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(cfg, leaveDelegate)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	assert.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.True(t, onStoppingCalled)
}

func TestTokensPersistencyDelegate_ShouldSkipTokensLoadingIfFileDoesNotExist(t *testing.T) {
	// Create a temporary file and immediately delete it.
	tokensFile, err := ioutil.TempFile(os.TempDir(), "tokens-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(tokensFile.Name()))

	testDelegate := &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
			assert.False(t, instanceExists)
			return JOINING, Tokens{1, 2, 3, 4, 5}
		},
	}

	persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, log.NewNopLogger())

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(cfg, persistencyDelegate)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, JOINING, lifecycler.GetState())
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

	// Ensure tokens have been stored.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, actualTokens)
}

func TestTokensPersistencyDelegate_ShouldLoadTokensFromFileIfFileExist(t *testing.T) {
	tokensFile, err := ioutil.TempFile(os.TempDir(), "tokens-*")
	require.NoError(t, err)
	defer os.Remove(tokensFile.Name()) //nolint:errcheck

	// Store some tokens to the file.
	storedTokens := Tokens{6, 7, 8, 9, 10}
	require.NoError(t, storedTokens.StoreToFile(tokensFile.Name()))

	testDelegate := &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
			assert.True(t, instanceExists)
			assert.Equal(t, ACTIVE, instanceDesc.GetState())
			assert.Equal(t, storedTokens, Tokens(instanceDesc.GetTokens()))

			return instanceDesc.GetState(), instanceDesc.GetTokens()
		},
	}

	persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, log.NewNopLogger())

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(cfg, persistencyDelegate)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, ACTIVE, lifecycler.GetState())
	assert.Equal(t, storedTokens, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

	// Ensure we can still read back the tokens file.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, storedTokens, actualTokens)
}

func TestTokensPersistencyDelegate_ShouldHandleTheCaseTheInstanceIsAlreadyInTheRing(t *testing.T) {
	storedTokens := Tokens{6, 7, 8, 9, 10}
	differentTokens := Tokens{1, 2, 3, 4, 5}

	tests := map[string]struct {
		storedTokens   Tokens
		initialState   IngesterState
		initialTokens  Tokens
		expectedState  IngesterState
		expectedTokens Tokens
	}{
		"instance already registered in the ring without tokens": {
			initialState:   PENDING,
			initialTokens:  nil,
			expectedState:  ACTIVE,
			expectedTokens: storedTokens,
		},
		"instance already registered in the ring with tokens": {
			initialState:   JOINING,
			initialTokens:  differentTokens,
			expectedState:  JOINING,
			expectedTokens: differentTokens,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tokensFile, err := ioutil.TempFile(os.TempDir(), "tokens-*")
			require.NoError(t, err)
			defer os.Remove(tokensFile.Name()) //nolint:errcheck

			// Store some tokens to the file.
			require.NoError(t, storedTokens.StoreToFile(tokensFile.Name()))

			testDelegate := &mockDelegate{
				onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
					return instanceDesc.GetState(), instanceDesc.GetTokens()
				},
			}

			persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, log.NewNopLogger())

			ctx := context.Background()
			cfg := prepareBasicLifecyclerConfig()
			lifecycler, store, err := prepareBasicLifecyclerWithDelegate(cfg, persistencyDelegate)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

			// Add the instance to the ring.
			require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
				ringDesc := NewDesc()
				ringDesc.AddIngester(cfg.ID, cfg.Addr, cfg.Zone, testData.initialTokens, testData.initialState)
				return ringDesc, true, nil
			}))

			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			assert.Equal(t, testData.expectedState, lifecycler.GetState())
			assert.Equal(t, testData.expectedTokens, lifecycler.GetTokens())
			assert.True(t, lifecycler.IsRegistered())
		})
	}
}

// TestDelegatesChain tests chaining all provided delegates together.
func TestDelegatesChain(t *testing.T) {
	onStoppingCalled := false

	// Create a temporary file and immediately delete it.
	tokensFile, err := ioutil.TempFile(os.TempDir(), "tokens-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(tokensFile.Name()))

	// Chain delegates together.
	var chain BasicLifecyclerDelegate
	chain = &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc IngesterDesc) (IngesterState, Tokens) {
			assert.False(t, instanceExists)
			return JOINING, Tokens{1, 2, 3, 4, 5}
		},
		onStopping: func(l *BasicLifecycler) {
			assert.Equal(t, LEAVING, l.GetState())
			onStoppingCalled = true
		},
	}

	chain = NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, chain, log.NewNopLogger())
	chain = NewLeaveOnStoppingDelegate(chain, log.NewNopLogger())

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(cfg, chain)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, JOINING, lifecycler.GetState())
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.True(t, onStoppingCalled)

	// Ensure tokens have been stored.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, actualTokens)
}
