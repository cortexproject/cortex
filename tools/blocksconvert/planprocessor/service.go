package planprocessor

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type PlanProcessor interface {
	// Returns "id" that is appended to "finished" status file.
	ProcessPlanEntries(ctx context.Context, entries chan blocksconvert.PlanEntry) (string, error)
}

type Config struct {
	// Exported config options.
	Name              string
	HeartbeatPeriod   time.Duration
	SchedulerEndpoint string
	NextPlanInterval  time.Duration
	GrpcConfig        grpcclient.Config
}

func (cfg *Config) RegisterFlags(prefix string, f *flag.FlagSet) {
	cfg.GrpcConfig.RegisterFlagsWithPrefix(prefix+".client", f)

	host, _ := os.Hostname()
	f.StringVar(&cfg.Name, prefix+".name", host, "Name passed to scheduler, defaults to hostname.")
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+".heartbeat", 5*time.Minute, "How often to update plan progress file.")
	f.StringVar(&cfg.SchedulerEndpoint, prefix+".scheduler-endpoint", "", "Scheduler endpoint to ask for more plans to work on.")
	f.DurationVar(&cfg.NextPlanInterval, prefix+".next-plan-interval", 1*time.Minute, "How often to ask for next plan (when idle)")
}

// Creates new plan processor service.
// PlansDirectory is used for storing plan files.
// Bucket client used for downloading plan files.
// Cleanup function called on startup and after each build. Can be nil.
// Factory for creating PlanProcessor. Called for each new plan.
func NewService(cfg Config, plansDirectory string, bucket objstore.Bucket, cleanup func(logger log.Logger) error, factory func(planLog log.Logger, userID string, dayStart, dayEnd time.Time) PlanProcessor, l log.Logger, reg prometheus.Registerer) (*Service, error) {
	if cfg.SchedulerEndpoint == "" {
		return nil, errors.New("no scheduler endpoint")
	}

	if bucket == nil || factory == nil {
		return nil, errors.New("invalid config")
	}

	if plansDirectory == "" {
		return nil, errors.New("no directory for plans")
	}
	if err := os.MkdirAll(plansDirectory, os.FileMode(0700)); err != nil {
		return nil, errors.Wrap(err, "failed to create plans directory")
	}

	b := &Service{
		cfg:            cfg,
		plansDirectory: plansDirectory,
		bucket:         bucket,
		cleanupFn:      cleanup,
		factory:        factory,
		log:            l,

		currentPlanStartTime: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_plan_start_time_seconds",
			Help: "Start time of current plan's time range (unix timestamp).",
		}),
		planFileReadPosition: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_plan_file_position",
			Help: "Read bytes from the plan file.",
		}),
		planFileSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_plan_size",
			Help: "Total size of plan file.",
		}),
	}
	b.Service = services.NewBasicService(b.cleanup, b.running, nil)
	return b, nil
}

// This service implements common behaviour for plan-processing: 1) wait for next plan, 2) download plan,
// 3) process each plan entry, 4) delete local plan, 5) repeat. It gets plans from scheduler. During plan processing,
// this service maintains "progress" status file, and when plan processing finishes, it uploads "finished" plan.
type Service struct {
	services.Service

	cfg Config
	log log.Logger

	plansDirectory string
	bucket         objstore.Bucket
	cleanupFn      func(logger log.Logger) error
	factory        func(planLog log.Logger, userID string, dayStart time.Time, dayEnd time.Time) PlanProcessor

	planFileReadPosition prometheus.Gauge
	planFileSize         prometheus.Gauge
	currentPlanStartTime prometheus.Gauge
}

func (s *Service) cleanup(_ context.Context) error {
	files, err := ioutil.ReadDir(s.plansDirectory)
	if err != nil {
		return err
	}

	for _, f := range files {
		toRemove := filepath.Join(s.plansDirectory, f.Name())

		level.Info(s.log).Log("msg", "deleting unfinished local plan file", "file", toRemove)
		err = os.Remove(toRemove)
		if err != nil {
			return errors.Wrapf(err, "removing %s", toRemove)
		}
	}

	if s.cleanupFn != nil {
		return s.cleanupFn(s.log)
	}
	return nil
}

func (s *Service) running(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.NextPlanInterval)
	defer ticker.Stop()

	var schedulerClient blocksconvert.SchedulerClient
	var conn *grpc.ClientConn

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			// We may get "tick" even when we should stop.
			if ctx.Err() != nil {
				return nil
			}

			if conn == nil {
				opts, err := s.cfg.GrpcConfig.DialOption(nil, nil)
				if err != nil {
					return err
				}

				conn, err = grpc.Dial(s.cfg.SchedulerEndpoint, opts...)
				if err != nil {
					level.Error(s.log).Log("msg", "failed to dial", "endpoint", s.cfg.SchedulerEndpoint, "err", err)
					continue
				}

				schedulerClient = blocksconvert.NewSchedulerClient(conn)
			}

			resp, err := schedulerClient.NextPlan(ctx, &blocksconvert.NextPlanRequest{Name: s.cfg.Name})
			if err != nil {
				level.Error(s.log).Log("msg", "failed to get next plan due to error, closing connection", "err", err)
				_ = conn.Close()
				conn = nil
				schedulerClient = nil
				continue
			}

			// No plan to work on, ignore.
			if resp.PlanFile == "" {
				continue
			}

			isPlanFile, planBaseName := blocksconvert.IsPlanFilename(resp.PlanFile)
			if !isPlanFile {
				level.Error(s.log).Log("msg", "got invalid plan file", "planFile", resp.PlanFile)
				continue
			}

			ok, base, _ := blocksconvert.IsProgressFilename(resp.ProgressFile)
			if !ok || base != planBaseName {
				level.Error(s.log).Log("msg", "got invalid progress file", "progressFile", resp.ProgressFile)
				continue
			}

			level.Info(s.log).Log("msg", "received plan file", "planFile", resp.PlanFile, "progressFile", resp.ProgressFile)

			err = s.downloadAndProcessPlanFile(ctx, resp.PlanFile, planBaseName, resp.ProgressFile)
			if err != nil {
				level.Error(s.log).Log("msg", "failed to process plan file", "planFile", resp.PlanFile, "err", err)

				// If context is canceled (blocksconvert is shutting down, or due to hearbeating failure), don't upload error.
				if !errors.Is(err, context.Canceled) {
					errorFile := blocksconvert.ErrorFilename(planBaseName)
					err = s.bucket.Upload(ctx, errorFile, strings.NewReader(err.Error()))
					if err != nil {
						level.Error(s.log).Log("msg", "failed to upload error file", "errorFile", errorFile, "err", err)
					}
				}
			}

			err = s.cleanup(ctx)
			if err != nil {
				level.Error(s.log).Log("msg", "failed to cleanup working directory", "err", err)
			}
		}
	}
}

func (s *Service) downloadAndProcessPlanFile(ctx context.Context, planFile, planBaseName, lastProgressFile string) error {
	defer s.planFileSize.Set(0)
	defer s.planFileReadPosition.Set(0)
	defer s.currentPlanStartTime.Set(0)

	planLog := log.With(s.log, "plan", planFile)

	// Start heartbeating (updating of progress file). We setup new context used for the rest of the function.
	// If hearbeating fails, we cancel this new context to abort quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	hb := newHeartbeat(planLog, s.bucket, s.cfg.HeartbeatPeriod, planBaseName, lastProgressFile)
	hb.AddListener(services.NewListener(nil, nil, nil, nil, func(from services.State, failure error) {
		level.Error(planLog).Log("msg", "heartbeating failed, aborting build", "failure", failure)
		cancel()
	}))
	if err := services.StartAndAwaitRunning(ctx, hb); err != nil {
		return errors.Wrap(err, "failed to start heartbeating")
	}

	localPlanFile := filepath.Join(s.plansDirectory, filepath.Base(planFile))
	planSize, err := downloadPlanFile(ctx, s.bucket, planFile, localPlanFile)
	if err != nil {
		return errors.Wrapf(err, "failed to download plan file %s to %s", planFile, localPlanFile)
	}
	level.Info(planLog).Log("msg", "downloaded plan file", "localPlanFile", localPlanFile, "size", planSize)

	s.planFileSize.Set(float64(planSize))

	f, err := os.Open(localPlanFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read local plan file %s", localPlanFile)
	}
	defer func() {
		_ = f.Close()
	}()

	// Use a buffer for reading plan file.
	r, err := blocksconvert.PreparePlanFileReader(planFile, bufio.NewReaderSize(&readPositionReporter{r: f, g: s.planFileReadPosition}, 1*1024*1024))
	if err != nil {
		return err
	}

	dec := json.NewDecoder(r)

	userID, dayStart, dayEnd, err := parsePlanHeader(dec)
	if err != nil {
		return err
	}

	s.currentPlanStartTime.Set(float64(dayStart.Unix()))

	level.Info(planLog).Log("msg", "processing plan file", "user", userID, "dayStart", dayStart, "dayEnd", dayEnd)

	processor := s.factory(planLog, userID, dayStart, dayEnd)

	planEntryCh := make(chan blocksconvert.PlanEntry)

	idChan := make(chan string, 1)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		id, err := processor.ProcessPlanEntries(gctx, planEntryCh)
		idChan <- id
		return err
	})
	g.Go(func() error {
		return parsePlanEntries(gctx, dec, planEntryCh)
	})

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to build block")
	}

	err = os.Remove(localPlanFile)
	if err != nil {
		level.Warn(planLog).Log("msg", "failed to delete local plan file", "err", err)
	}

	id := <-idChan

	// Upload finished status file
	finishedFile := blocksconvert.FinishedFilename(planBaseName, id)
	if err := s.bucket.Upload(ctx, finishedFile, strings.NewReader(id)); err != nil {
		return errors.Wrap(err, "failed to upload finished status file")
	}
	level.Info(planLog).Log("msg", "uploaded finished file", "file", finishedFile)

	// Stop heartbeating.
	if err := services.StopAndAwaitTerminated(ctx, hb); err != nil {
		// No need to report this error to caller to avoid generating error file.
		level.Warn(planLog).Log("msg", "hearbeating failed", "err", err)
	}

	// All OK
	return nil
}

func downloadPlanFile(ctx context.Context, bucket objstore.Bucket, planFile string, localPlanFile string) (int64, error) {
	f, err := os.Create(localPlanFile)
	if err != nil {
		return 0, err
	}

	r, err := bucket.Get(ctx, planFile)
	if err != nil {
		_ = f.Close()
		return 0, err
	}
	// Copy will read `r` until EOF, or error is returned. Any possible error from Close is irrelevant.
	defer func() { _ = r.Close() }()

	n, err := io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		return 0, err
	}

	return n, f.Close()
}

func parsePlanHeader(dec *json.Decoder) (userID string, startTime, endTime time.Time, err error) {
	header := blocksconvert.PlanEntry{}
	if err = dec.Decode(&header); err != nil {
		return
	}
	if header.User == "" || header.DayIndex == 0 {
		err = errors.New("failed to read plan file header: no user or day index found")
		return
	}

	dayStart := time.Unix(int64(header.DayIndex)*int64(24*time.Hour/time.Second), 0).UTC()
	dayEnd := dayStart.Add(24 * time.Hour)
	return header.User, dayStart, dayEnd, nil
}

func parsePlanEntries(ctx context.Context, dec *json.Decoder, planEntryCh chan blocksconvert.PlanEntry) error {
	defer close(planEntryCh)

	var err error
	complete := false
	entry := blocksconvert.PlanEntry{}
	for err = dec.Decode(&entry); err == nil; err = dec.Decode(&entry) {
		if entry.Complete {
			complete = true
			entry.Reset()
			continue
		}

		if complete {
			return errors.New("plan entries found after plan footer")
		}

		if entry.SeriesID != "" && len(entry.Chunks) > 0 {
			select {
			case planEntryCh <- entry:
				// ok
			case <-ctx.Done():
				return nil
			}

		}

		entry.Reset()
	}

	if err == io.EOF {
		if !complete {
			return errors.New("plan is not complete")
		}
		err = nil
	}
	return errors.Wrap(err, "parsing plan entries")
}

type readPositionReporter struct {
	r   io.Reader
	g   prometheus.Gauge
	pos int64
}

func (r *readPositionReporter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.pos += int64(n)
		r.g.Set(float64(r.pos))
	}
	return n, err
}
