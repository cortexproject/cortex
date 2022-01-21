package planprocessor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

	lastProgressFile string
}

func newHeartbeat(log log.Logger, bucket objstore.Bucket, interval time.Duration, planFileBasename, lastProgressFile string) *heartbeat {
	hb := &heartbeat{
		log:              log,
		bucket:           bucket,
		planFileBasename: planFileBasename,
		lastProgressFile: lastProgressFile,
	}

	hb.Service = services.NewTimerService(interval, hb.heartbeat, hb.heartbeat, hb.stopping)
	return hb
}

func (hb *heartbeat) heartbeat(ctx context.Context) error {
	if hb.lastProgressFile != "" {
		ok, err := hb.bucket.Exists(ctx, hb.lastProgressFile)
		if err != nil {
			level.Warn(hb.log).Log("msg", "failed to check last progress file", "err", err)
			return errors.Wrapf(err, "cannot check if progress file exists: %s", hb.lastProgressFile)
		}

		if !ok {
			level.Warn(hb.log).Log("msg", "previous progress file doesn't exist")
			return errors.Errorf("previous progress file doesn't exist: %s", hb.lastProgressFile)
		}
	}

	now := time.Now()
	newProgressFile := blocksconvert.ProgressFilename(hb.planFileBasename, now)
	if newProgressFile == hb.lastProgressFile {
		// when scheduler creates progress file, it can have the same timestamp.
		return nil
	}

	if err := hb.bucket.Upload(ctx, newProgressFile, strings.NewReader(strconv.FormatInt(now.Unix(), 10))); err != nil {
		return errors.Wrap(err, "failed to upload new progress file")
	}

	if hb.lastProgressFile != "" {
		if err := hb.bucket.Delete(ctx, hb.lastProgressFile); err != nil {
			return errors.Wrap(err, "failed to delete old progress file")
		}
	}

	level.Info(hb.log).Log("msg", "updated progress", "file", newProgressFile)
	hb.lastProgressFile = newProgressFile
	return nil
}

func (hb *heartbeat) stopping(failure error) error {
	// Only delete progress file if there was no failure until now.
	if failure != nil {
		return nil
	}

	level.Info(hb.log).Log("msg", "deleting last progress file", "file", hb.lastProgressFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if hb.lastProgressFile != "" {
		if err := hb.bucket.Delete(ctx, hb.lastProgressFile); err != nil {
			return errors.Wrap(err, "failed to delete last progress file")
		}
	}

	return nil
}

func (hb *heartbeat) String() string {
	return "heartbeat"
}
