package queryrange

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
)

const seconds = 1e3 // 1e3 milliseconds per second.

func TestNextDayBoundary(t *testing.T) {
	for i, tc := range []struct {
		in, step, out int64
	}{
		// Smallest possible period is 1 millisecond
		{0, 1, millisecondPerDay - 1},
		// A more standard example
		{0, 15 * seconds, millisecondPerDay - 15*seconds},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 15 * seconds, millisecondPerDay - (15-1)*seconds},
		// Move start time forward 14 seconds; end time moves the same
		{14 * seconds, 15 * seconds, millisecondPerDay - (15-14)*seconds},
		// Now some examples where the period does not divide evenly into a day:
		// 1 day modulus 35 seconds = 20 seconds
		{0, 35 * seconds, millisecondPerDay - 20*seconds},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 35 * seconds, millisecondPerDay - (20-1)*seconds},
		// If the end time lands exactly on midnight we stop one period before that
		{20 * seconds, 35 * seconds, millisecondPerDay - 35*seconds},
		// This example starts 35 seconds after the 5th one ends
		{millisecondPerDay + 15*seconds, 35 * seconds, 2*millisecondPerDay - 5*seconds},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.out, nextDayBoundary(tc.in, tc.step))
		})
	}
}

func TestSplitQuery(t *testing.T) {
	for i, tc := range []struct {
		input    *Request
		expected []*Request
	}{
		{
			input: &Request{
				Start: 0,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []*Request{
				{
					Start: 0,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
		},
		{
			input: &Request{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []*Request{
				{
					Start: 0,
					End:   24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
		},
		{
			input: &Request{
				Start: 0,
				End:   2 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []*Request{
				{
					Start: 0,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				{
					Start: 24 * 3600 * seconds,
					End:   2 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
		},
		{
			input: &Request{
				Start: 3 * 3600 * seconds,
				End:   3 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []*Request{
				{
					Start: 3 * 3600 * seconds,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				{
					Start: 24 * 3600 * seconds,
					End:   (2 * 24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				{
					Start: 2 * 24 * 3600 * seconds,
					End:   3 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			days := splitQuery(tc.input)
			require.Equal(t, tc.expected, days)
		})
	}
}

func TestSplitByDay(t *testing.T) {
	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(responseBody))
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	roundtripper := roundTripper{
		handler: splitByDay{
			next: ToRoundTripperMiddleware{
				Next: singleHostRoundTripper{
					host: u.Host,
					next: http.DefaultTransport,
				},
			},
			limits: fakeLimits{},
		},
		limits: fakeLimits{},
	}

	mergedResponse, err := mergeAPIResponses([]*APIResponse{
		parsedResponse,
		parsedResponse,
	})
	require.NoError(t, err)

	mergedHTTPResponse, err := mergedResponse.toHTTPResponse(context.Background())
	require.NoError(t, err)

	mergedHTTPResponseBody, err := ioutil.ReadAll(mergedHTTPResponse.Body)
	require.NoError(t, err)

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{query, string(mergedHTTPResponseBody)},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			req = req.WithContext(ctx)

			resp, err := roundtripper.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}
