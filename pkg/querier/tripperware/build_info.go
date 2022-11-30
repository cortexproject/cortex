package tripperware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/prometheus/common/version"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

type buildInfoResponse struct {
	Status string                `json:"status"`
	Data   *v1.PrometheusVersion `json:"data"`
}

// BuildInfoRoundTripper is a RoundTripper that returns static build info information.
type BuildInfoRoundTripper struct{}

// NewBuildInfoRoundTripper creates a round tripper which returns build info.
func NewBuildInfoRoundTripper() *BuildInfoRoundTripper {
	return &BuildInfoRoundTripper{}
}

// RoundTrip implements RoundTripper.
func (r *BuildInfoRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	infoResponse := buildInfoResponse{
		Status: "success",
		Data: &v1.PrometheusVersion{
			Version:   version.Version,
			Branch:    version.Branch,
			Revision:  version.Revision,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
	}
	output, err := json.Marshal(infoResponse)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{
		Body:       io.NopCloser(bytes.NewBuffer(output)),
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
	return resp, nil
}
