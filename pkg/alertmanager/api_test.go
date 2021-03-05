package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	util_log "github.com/cortexproject/cortex/pkg/util/log"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestAMConfigValidationAPI(t *testing.T) {
	testCases := []struct {
		name     string
		cfg      string
		response string
		err      error
	}{
		{
			name: "It is not a valid payload without receivers",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
`,
			err: fmt.Errorf("error validating Alertmanager config: undefined receiver \"default-receiver\" used in route"),
		},
		{
			name: "It is valid",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
`,
		},
		{
			name: "It is not valid with paths in the template",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: unable to create template file 'not/very/good.tpl'"),
		},
		{
			name: "It is not valid with .",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  ".": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: unable to create template file '.'"),
		},
		{
			name: "It is not valid if the config is empty due to wrong indendatation",
			cfg: `
alertmanager_config: |
route:
  receiver: 'default-receiver'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  group_by: [cluster, alertname]
receivers:
  - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
		{
			name: "It is not valid if the config is empty due to wrong key",
			cfg: `
XWRONGalertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
	}

	am := &MultitenantAlertmanager{
		store:  prepareInMemoryAlertStore(),
		logger: util_log.Logger,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://alertmanager/api/v1/alerts", bytes.NewReader([]byte(tc.cfg)))
			ctx := user.InjectOrgID(req.Context(), "testing")
			w := httptest.NewRecorder()
			am.SetUserConfig(w, req.WithContext(ctx))
			resp := w.Result()

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			if tc.err == nil {
				require.Equal(t, http.StatusCreated, resp.StatusCode)
				require.Equal(t, "", string(body))
			} else {
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				require.Equal(t, tc.err.Error()+"\n", string(body))
			}

		})
	}
}

func TestMultitenantAlertmanager_DeleteUserConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertStore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertStore,
		logger: util_log.Logger,
	}

	require.NoError(t, alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
		User:      "test_user",
		RawConfig: "config",
	}))

	require.Equal(t, 1, len(storage.Objects()))

	req := httptest.NewRequest("POST", "/multitenant_alertmanager/delete_tenant_config", nil)
	// Missing user returns error 401. (DeleteUserConfig does this, but in practice, authentication middleware will do it first)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, 1, len(storage.Objects()))
	}

	// With user in the context.
	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}

	// Repeating the request still reports 200
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}
}
