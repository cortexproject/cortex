package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

var (
	errorVisitMarkerNotFound  = errors.New("visit marker not found")
	errorUnmarshalVisitMarker = errors.New("unmarshal visit marker JSON")
)

type VisitStatus string

const (
	Pending    VisitStatus = "pending"
	InProgress VisitStatus = "inProgress"
	Completed  VisitStatus = "completed"
	Failed     VisitStatus = "failed"
)

type VisitMarker interface {
	GetVisitMarkerFilePath() string
	UpdateStatus(ownerIdentifier string, status VisitStatus)
	GetStatus() VisitStatus
	IsExpired(visitMarkerTimeout time.Duration) bool
	String() string
}

type VisitMarkerManager struct {
	bkt                    objstore.InstrumentedBucket
	logger                 log.Logger
	ownerIdentifier        string
	visitMarker            VisitMarker
	visitMarkerReadFailed  prometheus.Counter
	visitMarkerWriteFailed prometheus.Counter
}

func NewVisitMarkerManager(
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	ownerIdentifier string,
	visitMarker VisitMarker,
	visitMarkerReadFailed prometheus.Counter,
	visitMarkerWriteFailed prometheus.Counter,
) *VisitMarkerManager {
	return &VisitMarkerManager{
		bkt:                    bkt,
		logger:                 log.With(logger, "type", fmt.Sprintf("%T", visitMarker)),
		ownerIdentifier:        ownerIdentifier,
		visitMarker:            visitMarker,
		visitMarkerReadFailed:  visitMarkerReadFailed,
		visitMarkerWriteFailed: visitMarkerWriteFailed,
	}
}

func (v *VisitMarkerManager) HeartBeat(ctx context.Context, errChan <-chan error, visitMarkerFileUpdateInterval time.Duration, deleteOnExit bool) {
	level.Info(v.getLogger()).Log("msg", "start visit marker heart beat")
	ticker := time.NewTicker(visitMarkerFileUpdateInterval)
	defer ticker.Stop()
heartBeat:
	for {
		v.MarkInProgress(ctx)

		select {
		case <-ctx.Done():
			level.Warn(v.getLogger()).Log("msg", "visit marker heart beat got cancelled")
			v.MarkPending(context.Background())
			break heartBeat
		case <-ticker.C:
			continue
		case err := <-errChan:
			if err == nil {
				level.Info(v.getLogger()).Log("msg", "update visit marker to completed status")
				v.MarkCompleted(ctx)
			} else {
				level.Warn(v.getLogger()).Log("msg", "stop visit marker heart beat due to error", "err", err)
				if compact.IsHaltError(err) {
					level.Info(v.getLogger()).Log("msg", "update visit marker to failed status", "err", err)
					v.MarkFailed(ctx)
				} else {
					level.Info(v.getLogger()).Log("msg", "update visit marker to pending status", "err", err)
					v.MarkPending(ctx)
				}
			}
			break heartBeat
		}
	}
	level.Info(v.getLogger()).Log("msg", "stop visit marker heart beat")
	if deleteOnExit {
		level.Info(v.getLogger()).Log("msg", "delete visit marker when exiting heart beat")
		v.DeleteVisitMarker(context.Background())
	}
}

func (v *VisitMarkerManager) MarkInProgress(ctx context.Context) {
	v.visitMarker.UpdateStatus(v.ownerIdentifier, InProgress)
	if err := v.updateVisitMarker(ctx); err != nil {
		level.Error(v.getLogger()).Log("msg", "unable to upsert visit marker file content", "err", err)
		return
	}
	level.Debug(v.getLogger()).Log("msg", "marked in progress")
}

func (v *VisitMarkerManager) MarkPending(ctx context.Context) {
	v.visitMarker.UpdateStatus(v.ownerIdentifier, Pending)
	if err := v.updateVisitMarker(ctx); err != nil {
		level.Error(v.getLogger()).Log("msg", "unable to upsert visit marker file content", "err", err)
		return
	}
	level.Debug(v.getLogger()).Log("msg", "marked pending")
}

func (v *VisitMarkerManager) MarkCompleted(ctx context.Context) {
	v.visitMarker.UpdateStatus(v.ownerIdentifier, Completed)
	if err := v.updateVisitMarker(ctx); err != nil {
		level.Error(v.getLogger()).Log("msg", "unable to upsert visit marker file content", "err", err)
		return
	}
	level.Debug(v.getLogger()).Log("msg", "marked completed")
}

func (v *VisitMarkerManager) MarkFailed(ctx context.Context) {
	v.visitMarker.UpdateStatus(v.ownerIdentifier, Failed)
	if err := v.updateVisitMarker(ctx); err != nil {
		level.Error(v.getLogger()).Log("msg", "unable to upsert visit marker file content", "err", err)
		return
	}
	level.Debug(v.getLogger()).Log("msg", "marked failed")
}

func (v *VisitMarkerManager) DeleteVisitMarker(ctx context.Context) {
	if err := v.bkt.Delete(ctx, v.visitMarker.GetVisitMarkerFilePath()); err != nil {
		level.Error(v.getLogger()).Log("msg", "failed to delete visit marker", "err", err)
		return
	}
	level.Debug(v.getLogger()).Log("msg", "visit marker deleted")
}

func (v *VisitMarkerManager) ReadVisitMarker(ctx context.Context, visitMarker any) error {
	visitMarkerFile := v.visitMarker.GetVisitMarkerFilePath()
	visitMarkerFileReader, err := v.bkt.ReaderWithExpectedErrs(v.bkt.IsObjNotFoundErr).Get(ctx, visitMarkerFile)
	if err != nil {
		if v.bkt.IsObjNotFoundErr(err) {
			return errors.Wrapf(errorVisitMarkerNotFound, "visit marker file: %s", visitMarkerFile)
		}
		v.visitMarkerReadFailed.Inc()
		return errors.Wrapf(err, "get visit marker file: %s", visitMarkerFile)
	}
	defer runutil.CloseWithLogOnErr(v.getLogger(), visitMarkerFileReader, "close visit marker reader")
	b, err := io.ReadAll(visitMarkerFileReader)
	if err != nil {
		v.visitMarkerReadFailed.Inc()
		return errors.Wrapf(err, "read visit marker file: %s", visitMarkerFile)
	}
	if err = json.Unmarshal(b, visitMarker); err != nil {
		v.visitMarkerReadFailed.Inc()
		return errors.Wrapf(errorUnmarshalVisitMarker, "visit marker file: %s, content: %s, error: %v", visitMarkerFile, string(b), err.Error())
	}
	level.Debug(v.getLogger()).Log("msg", "visit marker read from file", "visit_marker_file", visitMarkerFile)
	return nil
}

func (v *VisitMarkerManager) updateVisitMarker(ctx context.Context) error {
	visitMarkerFileContent, err := json.Marshal(v.visitMarker)
	if err != nil {
		v.visitMarkerWriteFailed.Inc()
		return err
	}

	reader := bytes.NewReader(visitMarkerFileContent)
	if err := v.bkt.Upload(ctx, v.visitMarker.GetVisitMarkerFilePath(), reader); err != nil {
		v.visitMarkerWriteFailed.Inc()
		return err
	}
	return nil
}

func (v *VisitMarkerManager) getLogger() log.Logger {
	return log.With(v.logger, "visit_marker", v.visitMarker.String())
}
