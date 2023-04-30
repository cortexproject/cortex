package importer

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/cortexproject/cortex/pkg/importer/importerpb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type Importer struct {
	services.Service
}

func New() *Importer {
	i := &Importer{}
	i.Service = services.NewBasicService(i.starting, i.running, i.stopping)
	return i
}

func (i *Importer) SampleRPC(ctx context.Context, request *importerpb.SampleRequest) (*importerpb.SampleResponse, error) {
	//TODO implement me
	panic("implement me")
}

type BlockUrl struct {
	url string `json:"url"`
}

func (c *Importer) ImportHandler(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("hello world"))

	// Read the compressed block from the request body
	compressedBlock, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	// Create a temporary file for the compressed block
	compressedBlockFile, err := ioutil.TempFile("", "prom_block_*.gz")
	if err != nil {
		http.Error(w, "Error creating temporary file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(compressedBlockFile.Name())
	defer compressedBlockFile.Close()

	// Write the compressed block to the temporary file
	_, err = compressedBlockFile.Write(compressedBlock)
	if err != nil {
		http.Error(w, "Wrror writing to the temporary file", http.StatusInternalServerError)
		return
	}

	// open the compressed block as a gzip file
	compressedBlockReader, err := gzip.NewReader(compressedBlockFile)
	if err != nil {
		http.Error(w, "Error opening compressed block as gzip file", http.StatusInternalServerError)
		return
	}
	defer compressedBlockFile.Close()

	// Create a temporary directory for the uncompressed block
	uncompressedBlockDir, err := ioutil.TempDir("", "prom_block_*")
	if err != nil {
		http.Error(w, "Error creating the temporary directory", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(uncompressedBlockDir)

	// Extract the contents of the compressed block to the temporary directory
	tarReader := tar.NewReader(compressedBlockReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "Error reading the tar file", http.StatusInternalServerError)
			return
		}
		targetPath := filepath.Join(uncompressedBlockDir, header.Name)
		if header.FileInfo().IsDir() {
			err := os.MkdirAll(targetPath, 0755)
			if err != nil {
				http.Error(w, "Error creating directory in target path", http.StatusInternalServerError)
				return
			}
		} else {
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				http.Error(w, "Error oprning file in target path", http.StatusInternalServerError)
				return
			}
			defer file.Close()
			if _, err := io.Copy(file, tarReader); err != nil {
				http.Error(w, "Error extracting file to target path", http.StatusInternalServerError)
				return
			}
		}
	}

	// Check if the uncompressed block directory contains a valid prometheus TSDB block
	blockDirExists, err := checkBlockDirectory(uncompressedBlockDir)
	if err != nil {
		http.Error(w, "Error checking the block directory", http.StatusInternalServerError)
		return
	}
	if !blockDirExists {
		http.Error(w, "Invalid Prometheus TSDB block", http.StatusInternalServerError)
		return
	}

	// Move the uncompressed block directory to the final location in /tmp/tsdb_blocks
	finalBlockDir := filepath.Join("/tmp/tsdb_blocks", filepath.Base(uncompressedBlockDir))
	err = os.Rename(uncompressedBlockDir, finalBlockDir)
	if err != nil {
		http.Error(w, "Error moving block directory to final location", http.StatusInternalServerError)
		return
	}

	// Send a success response
	fmt.Fprintf(w, "Block imported successfully to %s\n", finalBlockDir)
	w.WriteHeader(http.StatusNoContent)
}

// checkBlockDirectory checks if the gives directory contains a valid Prometheus TSDB Block
func checkBlockDirectory(dirPath string) (bool, error) {
	// Check if the directory contains a "chunks_head" file
	_, err := os.Stat(filepath.Join(dirPath, "chunks_head"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// check if the directory contains a "meta.json" file
	_, err = os.Stat(filepath.Join(dirPath, "meta.json"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// check if the directory contains at least one index directory with a numeric name
	indexDirs, err := filepath.Glob(filepath.Join(dirPath, "index-*"))
	if err != nil {
		return false, err
	}
	if len(indexDirs) == 0 {
		return false, nil
	}

	// Check if each "index" directory contains a "blocks" subdirectory
	for _, indexDir := range indexDirs {
		blocksDir := filepath.Join(indexDir, "blocks")
		if _, err := os.Stat(blocksDir); os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (i *Importer) starting(ctx context.Context) error {
	return nil
}

func (i *Importer) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}

func (i *Importer) stopping(err error) error {
	return nil
}
