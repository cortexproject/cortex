package main

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_backendResponse_succeeded(t *testing.T) {
	tests := map[string]struct {
		resStatus int
		resError  error
		expected  bool
	}{
		"Error while executing request": {
			resStatus: 0,
			resError:  errors.New("network error"),
			expected:  false,
		},
		"2xx response status code": {
			resStatus: 200,
			resError:  nil,
			expected:  true,
		},
		"3xx response status code": {
			resStatus: 300,
			resError:  nil,
			expected:  false,
		},
		"4xx response status code": {
			resStatus: 400,
			resError:  nil,
			expected:  true,
		},
		"5xx response status code": {
			resStatus: 500,
			resError:  nil,
			expected:  false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := &backendResponse{
				status: testData.resStatus,
				err:    testData.resError,
			}

			assert.Equal(t, testData.expected, res.succeeded())
		})
	}
}

func Test_backendResponse_statusCode(t *testing.T) {
	tests := map[string]struct {
		resStatus int
		resError  error
		expected  int
	}{
		"Error while executing request": {
			resStatus: 0,
			resError:  errors.New("network error"),
			expected:  500,
		},
		"200 response status code": {
			resStatus: 200,
			resError:  nil,
			expected:  200,
		},
		"503 response status code": {
			resStatus: 503,
			resError:  nil,
			expected:  503,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := &backendResponse{
				status: testData.resStatus,
				err:    testData.resError,
			}

			assert.Equal(t, testData.expected, res.statusCode())
		})
	}
}
