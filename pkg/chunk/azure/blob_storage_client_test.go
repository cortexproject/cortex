package azure

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestGetObject(t *testing.T) {
	cfg := BlobStorageConfig{
		CloudEnvironment: "AzureGlobal",
		ContainerName:    "containername",
		AccountName:      "accountname",
		AccountKey: flagext.Secret{
			Value: "accountkey",
		},
	}
	log.Print("run in ", cfg.CloudEnvironment)
	tests := []struct {
		filename string
		expected string
	}{
		{
			"success.sh",
			"#!/bin/bash",
		},
		{
			"fail.sh",
			"#!/bin/bash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			bsClient, err := NewBlobStorage(&cfg, "")
			require.NoError(t, err)
			readCloser, err := bsClient.GetObject(context.Background(), tt.filename)
			require.NoError(t, err)
			buffer := make([]byte, 1000)
			_, err = readCloser.Read(buffer)
			if err != io.EOF {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, strings.Trim(string(buffer), "\n\x00"))
		})
	}
}

func TestPutObject(t *testing.T) {
	cfg := BlobStorageConfig{
		CloudEnvironment: "AzureGlobal",
		ContainerName:    "containername",
		AccountName:      "accountname",
		AccountKey: flagext.Secret{
			Value: "accountkey",
		},
		UploadBufferCount: 1,
		UploadBufferSize:  256000,
	}
	log.Print("run in ", cfg.CloudEnvironment)
	tests := []struct {
		name     string
		filename string
	}{
		{
			"success.sh",
			"/Users/binbin.zou/Desktop/success.sh",
		},
		{
			"fail.sh",
			"/Users/binbin.zou/Desktop/fail.sh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsClient, err := NewBlobStorage(&cfg, "")
			require.NoError(t, err)
			bytearr, err := ioutil.ReadFile(tt.filename)
			require.NoError(t, err)
			err = bsClient.PutObject(context.Background(), tt.name, bytes.NewReader(bytearr))
			require.NoError(t, err)
		})
	}
}
