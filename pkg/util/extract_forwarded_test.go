package util

import (
	"context"
	"net/http"
	"testing"
)

func TestGetSourceFromCtx(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "No value in key",
			value: "",
			want:  "",
		},
		{
			name:  "Value in key",
			value: "172.16.1.1",
			want:  "172.16.1.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.value != "" {
				ctx = NewSourceContext(ctx, tt.value)
			}
			if got := GetSourceFromCtx(ctx); got != tt.want {
				t.Errorf("GetSourceFromCtx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSource(t *testing.T) {
	type args struct {
		req *http.Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty forwarded for",
			args: args{
				req: &http.Request{RemoteAddr: "192.168.1.100:3454"},
			},
			want: "192.168.1.100",
		},
		{
			name: "single forward address",
			args: args{
				req: &http.Request{
					RemoteAddr: "192.168.1.100:3454",
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1"},
					},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "single forward address no RemoteAddr",
			args: args{
				req: &http.Request{
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1"},
					},
				},
			},
			want: "172.16.1.1",
		},
		{
			name: "multiple forward with remote",
			args: args{
				req: &http.Request{
					RemoteAddr: "192.168.1.100:3454",
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1, 10.10.13.20"},
					},
				},
			},
			want: "172.16.1.1, 10.10.13.20, 192.168.1.100",
		},
		{
			name: "no forward header, no remote",
			args: args{
				req: &http.Request{},
			},
			want: "",
		},
		{
			name: "remote has no port",
			args: args{
				req: &http.Request{
					RemoteAddr: "192.168.1.100",
				},
			},
			want: "192.168.1.100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSource(tt.args.req); got != tt.want {
				t.Errorf("GetSource() = %v, want %v", got, tt.want)
			}
		})
	}
}
