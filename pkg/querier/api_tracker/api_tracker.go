package api_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
)

type APITracker struct {
	mmappedFile   []byte
	getNextIndex  chan int
	logger        *slog.Logger
	closer        io.Closer
	maxConcurrent int
}

var _ io.Closer = &APITracker{}

const (
	apiEntrySize int = 1000
)

func parseAPIBrokenJSON(brokenJSON []byte) (string, bool) {
	apis := strings.ReplaceAll(string(brokenJSON), "\x00", "")
	if len(apis) > 0 {
		apis = apis[:len(apis)-1] + "]"
	}

	if len(apis) <= 1 {
		return "[]", false
	}

	return apis, true
}

func logUnfinishedAPIs(filename string, filesize int, logger *slog.Logger) {
	if _, err := os.Stat(filename); err == nil {
		fd, err := os.Open(filename)
		if err != nil {
			logger.Error("Failed to open API log file", "err", err)
			return
		}
		defer fd.Close()

		brokenJSON := make([]byte, filesize)
		_, err = fd.Read(brokenJSON)
		if err != nil {
			logger.Error("Failed to read API log file", "err", err)
			return
		}

		apis, apisExist := parseAPIBrokenJSON(brokenJSON)
		if !apisExist {
			return
		}
		logger.Info("These API calls didn't finish in prometheus' last run:", "apis", apis)
	}
}

type mmappedAPIFile struct {
	f io.Closer
	m mmap.MMap
}

func (f *mmappedAPIFile) Close() error {
	err := f.m.Unmap()
	if err != nil {
		err = fmt.Errorf("mmappedAPIFile: unmapping: %w", err)
	}
	if fErr := f.f.Close(); fErr != nil {
		return errors.Join(fmt.Errorf("close mmappedAPIFile.f: %w", fErr), err)
	}

	return err
}

func getAPIMMappedFile(filename string, filesize int, logger *slog.Logger) ([]byte, io.Closer, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		absPath, pathErr := filepath.Abs(filename)
		if pathErr != nil {
			absPath = filename
		}
		logger.Error("Error opening API log file", "file", absPath, "err", err)
		return nil, nil, err
	}

	err = file.Truncate(int64(filesize))
	if err != nil {
		file.Close()
		logger.Error("Error setting filesize.", "filesize", filesize, "err", err)
		return nil, nil, err
	}

	fileAsBytes, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		file.Close()
		logger.Error("Failed to mmap", "file", filename, "Attempted size", filesize, "err", err)
		return nil, nil, err
	}

	return fileAsBytes, &mmappedAPIFile{f: file, m: fileAsBytes}, err
}

func NewAPITracker(localStoragePath string, maxConcurrent int, logger *slog.Logger) *APITracker {
	if localStoragePath == "" {
		return nil
	}

	err := os.MkdirAll(localStoragePath, 0o777)
	if err != nil {
		logger.Error("Failed to create directory for logging active APIs")
		return nil
	}

	filename, filesize := filepath.Join(localStoragePath, "apis.active"), 1+maxConcurrent*apiEntrySize
	logUnfinishedAPIs(filename, filesize, logger)

	fileAsBytes, closer, err := getAPIMMappedFile(filename, filesize, logger)
	if err != nil {
		logger.Error("Unable to create mmap-ed active API log", "err", err)
		return nil
	}

	copy(fileAsBytes, "[")
	apiTracker := &APITracker{
		mmappedFile:   fileAsBytes,
		closer:        closer,
		getNextIndex:  make(chan int, maxConcurrent),
		logger:        logger,
		maxConcurrent: maxConcurrent,
	}

	apiTracker.generateIndices(maxConcurrent)

	return apiTracker
}

func newAPIJSONEntry(entryMap map[string]interface{}, logger *slog.Logger) []byte {
	jsonEntry, err := json.Marshal(entryMap)
	if err != nil {
		logger.Error("Cannot create json of API entry", "entry", entryMap)
		return []byte{}
	}

	if len(jsonEntry) > apiEntrySize {
		newEntryMap := make(map[string]interface{})
		newEntryMap["timestamp_sec"] = time.Now().Unix()
		jsonEntry, err = json.Marshal(newEntryMap)
		if err != nil {
			logger.Error("Cannot create json of API entry", "entry", newEntryMap)
			return []byte{}
		}
	}

	return jsonEntry
}

func (tracker *APITracker) generateIndices(maxConcurrent int) {
	for i := 0; i < maxConcurrent; i++ {
		tracker.getNextIndex <- 1 + (i * apiEntrySize)
	}
}

func (tracker *APITracker) Delete(insertIndex int) {
	copy(tracker.mmappedFile[insertIndex:], strings.Repeat("\x00", apiEntrySize))
	tracker.getNextIndex <- insertIndex
}

func (tracker *APITracker) Insert(ctx context.Context, entryMap map[string]interface{}) (int, error) {
	select {
	case i := <-tracker.getNextIndex:
		fileBytes := tracker.mmappedFile
		entry := newAPIJSONEntry(entryMap, tracker.logger)
		start, end := i, i+apiEntrySize

		copy(fileBytes[start:], entry)
		copy(fileBytes[end-1:], ",")
		return i, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (tracker *APITracker) Close() error {
	if tracker == nil || tracker.closer == nil {
		return nil
	}
	if err := tracker.closer.Close(); err != nil {
		return fmt.Errorf("close APITracker.closer: %w", err)
	}
	return nil
}
