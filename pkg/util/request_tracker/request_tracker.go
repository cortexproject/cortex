package request_tracker

import (
	"context"
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

type RequestTracker struct {
	mmappedFile   []byte
	getNextIndex  chan int
	logger        *slog.Logger
	closer        io.Closer
	maxConcurrent int
}

var _ io.Closer = &RequestTracker{}

const (
	maxEntrySize int = 1000
)

func parseRequestBrokenJSON(brokenJSON []byte) (string, bool) {
	requests := strings.ReplaceAll(string(brokenJSON), "\x00", "")
	if len(requests) > 0 {
		requests = requests[:len(requests)-1] + "]"
	}

	if len(requests) <= 1 {
		return "[]", false
	}

	return requests, true
}

func logUnfinishedRequests(filename string, filesize int, logger *slog.Logger) {
	if _, err := os.Stat(filename); err == nil {
		fd, err := os.Open(filename)
		if err != nil {
			logger.Error("Failed to open request log file", "err", err)
			return
		}
		defer fd.Close()

		brokenJSON := make([]byte, filesize)
		_, err = fd.Read(brokenJSON)
		if err != nil {
			logger.Error("Failed to read request log file", "err", err)
			return
		}

		requests, requestsExist := parseRequestBrokenJSON(brokenJSON)
		if !requestsExist {
			return
		}
		logger.Info("These requests didn't finish in cortex's last run:", "requests", requests)
	}
}

type mmappedRequestFile struct {
	f io.Closer
	m mmap.MMap
}

func (f *mmappedRequestFile) Close() error {
	err := f.m.Unmap()
	if err != nil {
		err = fmt.Errorf("mmappedRequestFile: unmapping: %w", err)
	}
	if fErr := f.f.Close(); fErr != nil {
		return errors.Join(fmt.Errorf("close mmappedRequestFile.f: %w", fErr), err)
	}

	return err
}

func getRequestMMappedFile(filename string, filesize int, logger *slog.Logger) ([]byte, io.Closer, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		absPath, pathErr := filepath.Abs(filename)
		if pathErr != nil {
			absPath = filename
		}
		logger.Error("Error opening request log file", "file", absPath, "err", err)
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

	return fileAsBytes, &mmappedRequestFile{f: file, m: fileAsBytes}, err
}

func NewRequestTracker(localStoragePath string, fileName string, maxConcurrent int, logger *slog.Logger) *RequestTracker {
	if localStoragePath == "" {
		return nil
	}

	err := os.MkdirAll(localStoragePath, 0o777)
	if err != nil {
		logger.Error("Failed to create directory for logging active requests")
		return nil
	}

	filename, filesize := filepath.Join(localStoragePath, fileName), 1+maxConcurrent*maxEntrySize
	logUnfinishedRequests(filename, filesize, logger)

	fileAsBytes, closer, err := getRequestMMappedFile(filename, filesize, logger)
	if err != nil {
		logger.Error("Unable to create mmap-ed active request log", "err", err)
		return nil
	}

	copy(fileAsBytes, "[")
	requestTracker := &RequestTracker{
		mmappedFile:   fileAsBytes,
		closer:        closer,
		getNextIndex:  make(chan int, maxConcurrent),
		logger:        logger,
		maxConcurrent: maxConcurrent,
	}

	requestTracker.generateIndices(maxConcurrent)

	return requestTracker
}

func (tracker *RequestTracker) generateIndices(maxConcurrent int) {
	for i := 0; i < maxConcurrent; i++ {
		tracker.getNextIndex <- 1 + (i * maxEntrySize)
	}
}

func (tracker *RequestTracker) Delete(insertIndex int) {
	copy(tracker.mmappedFile[insertIndex:], strings.Repeat("\x00", maxEntrySize))
	tracker.getNextIndex <- insertIndex
}

func (tracker *RequestTracker) Insert(ctx context.Context, entry []byte) (int, error) {
	if len(entry) > maxEntrySize {
		entry = generateMinEntry()
	}
	select {
	case i := <-tracker.getNextIndex:
		fileBytes := tracker.mmappedFile
		start, end := i, i+maxEntrySize

		copy(fileBytes[start:], entry)
		copy(fileBytes[end-1:], ",")
		return i, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func generateMinEntry() []byte {
	entryMap := make(map[string]interface{})
	entryMap["timestamp_sec"] = time.Now().Unix()
	return generateJSONEntry(entryMap)
}

func (tracker *RequestTracker) Close() error {
	if tracker == nil || tracker.closer == nil {
		return nil
	}
	if err := tracker.closer.Close(); err != nil {
		return fmt.Errorf("close RequestTracker.closer: %w", err)
	}
	return nil
}
