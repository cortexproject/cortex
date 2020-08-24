package builder

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/util/services"
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
	newHeartbeat := ""

	now := time.Now().Unix()
	newHeartbeat = fmt.Sprintf("%s.progress.%d", hb.planFileBasename, now)
	if err := hb.bucket.Upload(ctx, newHeartbeat, strings.NewReader(strconv.FormatInt(now, 10))); err != nil {
		return errors.Wrap(err, "failed to upload new heartbeat file")
	}

	if hb.lastHeartbeat != "" {
		if err := hb.bucket.Delete(ctx, hb.lastHeartbeat); err != nil {
			return errors.Wrap(err, "failed to delete old heartbeat file")
		}
	}

	hb.lastHeartbeat = newHeartbeat
	return nil
}

func (hb *heartbeat) stopping(_ error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if hb.lastHeartbeat != "" {
		if err := hb.bucket.Delete(ctx, hb.lastHeartbeat); err != nil {
			return errors.Wrap(err, "failed to delete old heartbeat file")
		}
	}

	return nil
}
