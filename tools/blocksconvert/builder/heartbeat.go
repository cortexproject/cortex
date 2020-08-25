package builder

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type heartbeat struct {
	services.Service

	log              log.Logger
	bucket           objstore.Bucket
	planFileBasename string

	lastHeartbeat string
}

func newHeartbeat(log log.Logger, bucket objstore.Bucket, interval time.Duration, planFileBasename, lastHeartbeat string) *heartbeat {
	hb := &heartbeat{
		log:              log,
		bucket:           bucket,
		planFileBasename: planFileBasename,
		lastHeartbeat:    lastHeartbeat,
	}

	hb.Service = services.NewTimerService(interval, hb.heartbeat, hb.heartbeat, hb.stopping)
	return hb
}

func (hb *heartbeat) heartbeat(ctx context.Context) error {
	if hb.lastHeartbeat != "" {
		ok, err := hb.bucket.Exists(ctx, hb.lastHeartbeat)
		if err != nil {
			level.Warn(hb.log).Log("msg", "failed to check old heartbeat file", "err", err)
			return errors.Wrap(err, "cannot check if heartbeat file exists")
		}

		if !ok {
			level.Warn(hb.log).Log("msg", "previous heartbeat file doesn't exist")
			return errors.New("previous heartbeat file doesn't exist")
		}
	}

	newHeartbeat := ""

	now := time.Now()
	newHeartbeat = blocksconvert.ProgressFile(hb.planFileBasename, now)
	if err := hb.bucket.Upload(ctx, newHeartbeat, strings.NewReader(strconv.FormatInt(now.Unix(), 10))); err != nil {
		return errors.Wrap(err, "failed to upload new heartbeat file")
	}

	if hb.lastHeartbeat != "" {
		if err := hb.bucket.Delete(ctx, hb.lastHeartbeat); err != nil {
			return errors.Wrap(err, "failed to delete old heartbeat file")
		}
	}

	level.Debug(hb.log).Log("msg", "updated heartbeat", "file", hb.lastHeartbeat)
	hb.lastHeartbeat = newHeartbeat
	return nil
}

func (hb *heartbeat) stopping(failure error) error {
	// Only delete heartbeat file if there was no failure until now.
	if failure != nil {
		return nil
	}

	level.Debug(hb.log).Log("msg", "deleting last heartbeat file", "file", hb.lastHeartbeat)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if hb.lastHeartbeat != "" {
		if err := hb.bucket.Delete(ctx, hb.lastHeartbeat); err != nil {
			return errors.Wrap(err, "failed to delete old heartbeat file")
		}
	}

	return nil
}

func (hb *heartbeat) String() string {
	return "heartbeat"
}
