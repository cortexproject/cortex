package scheduler

import (
	"context"
	"flag"
	"html/template"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type Config struct {
	ScanInterval        time.Duration
	PlanScanConcurrency int
	MaxProgressFileAge  time.Duration
	AllowedUsers        string
	IgnoredUserPattern  string
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.ScanInterval, "scheduler.scan-interval", 5*time.Minute, "How often to scan for plans and their status.")
	f.IntVar(&cfg.PlanScanConcurrency, "scheduler.plan-scan-concurrency", 5, "Limit of concurrent plan scans.")
	f.DurationVar(&cfg.MaxProgressFileAge, "scheduler.max-progress-file-age", 30*time.Minute, "Progress files older than this duration are deleted.")
	f.StringVar(&cfg.AllowedUsers, "scheduler.allowed-users", "", "Allowed users that can be converted, comma-separated")
	f.StringVar(&cfg.IgnoredUserPattern, "scheduler.ignore-users-regex", "", "If set and user ID matches this regex pattern, it will be ignored. Checked after applying -scheduler.allowed-users, if set.")
}

func NewScheduler(cfg Config, scfg blocksconvert.SharedConfig, l log.Logger, reg prometheus.Registerer, http *mux.Router, grpcServ *grpc.Server) (*Scheduler, error) {
	b, err := scfg.GetBucket(l, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create bucket")
	}

	var users = blocksconvert.AllowAllUsers
	if cfg.AllowedUsers != "" {
		users = blocksconvert.ParseAllowedUsers(cfg.AllowedUsers)
	}

	var ignoredUserRegex *regexp.Regexp = nil
	if cfg.IgnoredUserPattern != "" {
		re, err := regexp.Compile(cfg.IgnoredUserPattern)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile ignored user regex")
		}
		ignoredUserRegex = re
	}

	s := newSchedulerWithBucket(l, b, scfg.BucketPrefix, users, ignoredUserRegex, cfg, reg)
	blocksconvert.RegisterSchedulerServer(grpcServ, s)
	http.HandleFunc("/plans", s.httpPlans)
	return s, nil
}

func newSchedulerWithBucket(l log.Logger, b objstore.Bucket, bucketPrefix string, users blocksconvert.AllowedUsers, ignoredUsers *regexp.Regexp, cfg Config, reg prometheus.Registerer) *Scheduler {
	s := &Scheduler{
		log:          l,
		cfg:          cfg,
		bucket:       b,
		bucketPrefix: bucketPrefix,
		allowedUsers: users,
		ignoredUsers: ignoredUsers,

		planStatus: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scheduler_scanned_plans",
			Help: "Number of plans in different status",
		}, []string{"status"}),
		queuedPlansGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scheduler_queued_plans",
			Help: "Number of queued plans",
		}),
		oldestPlanTimestamp: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scheduler_oldest_queued_plan_seconds",
			Help: "Unix timestamp of oldest plan.",
		}),
		newestPlanTimestamp: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scheduler_newest_queued_plan_seconds",
			Help: "Unix timestamp of newest plan",
		}),
	}

	s.Service = services.NewTimerService(cfg.ScanInterval, s.scanBucketForPlans, s.scanBucketForPlans, nil)
	return s
}

type Scheduler struct {
	services.Service
	cfg Config
	log log.Logger

	allowedUsers blocksconvert.AllowedUsers
	ignoredUsers *regexp.Regexp // Can be nil.

	bucket       objstore.Bucket
	bucketPrefix string

	planStatus          *prometheus.GaugeVec
	queuedPlansGauge    prometheus.Gauge
	oldestPlanTimestamp prometheus.Gauge
	newestPlanTimestamp prometheus.Gauge

	// Used to avoid scanning while there is dequeuing happening.
	dequeueWG sync.WaitGroup

	scanMu       sync.Mutex
	scanning     bool
	allUserPlans map[string]map[string]plan
	plansQueue   []queuedPlan // Queued plans are sorted by day index - more recent (higher day index) days go first.
}

type queuedPlan struct {
	DayIndex int
	PlanFile string
}

func (s *Scheduler) scanBucketForPlans(ctx context.Context) error {
	s.scanMu.Lock()
	s.scanning = true
	s.scanMu.Unlock()

	defer func() {
		s.scanMu.Lock()
		s.scanning = false
		s.scanMu.Unlock()
	}()

	// Make sure that no dequeuing is happening when scanning.
	// This is to avoid race when dequeing creates progress file, but scan will not find it.
	s.dequeueWG.Wait()

	level.Info(s.log).Log("msg", "scanning for users")

	users, err := scanForUsers(ctx, s.bucket, s.bucketPrefix)
	if err != nil {
		level.Error(s.log).Log("msg", "failed to scan for users", "err", err)
		return nil
	}

	allUsers := len(users)
	users = s.allowedUsers.GetAllowedUsers(users)
	users = s.ignoreUsers(users)

	level.Info(s.log).Log("msg", "found users", "all", allUsers, "allowed", len(users))

	var mu sync.Mutex
	allPlans := map[string]map[string]plan{}
	stats := map[planStatus]int{}
	for _, k := range []planStatus{New, InProgress, Finished, Error, Invalid} {
		stats[k] = 0
	}
	var queue []queuedPlan

	runConcurrently(ctx, s.cfg.PlanScanConcurrency, users, func(user string) {
		userPrefix := path.Join(s.bucketPrefix, user) + "/"

		userPlans, err := scanForPlans(ctx, s.bucket, userPrefix)
		if err != nil {
			level.Error(s.log).Log("msg", "failed to scan plans for user", "user", user, "err", err)
			return
		}

		mu.Lock()
		allPlans[user] = map[string]plan{}
		mu.Unlock()

		for base, plan := range userPlans {
			st := plan.Status()
			if st == InProgress {
				s.deleteObsoleteProgressFiles(ctx, &plan, path.Join(userPrefix, base))

				// After deleting old progress files, status might have changed from InProgress to Error.
				st = plan.Status()
			}

			mu.Lock()
			allPlans[user][base] = plan
			stats[st]++
			mu.Unlock()

			if st != New {
				continue
			}

			dayIndex, err := strconv.ParseInt(base, 10, 32)
			if err != nil {
				level.Warn(s.log).Log("msg", "unable to parse day-index", "planFile", plan.PlanFiles[0])
				continue
			}

			mu.Lock()
			queue = append(queue, queuedPlan{
				DayIndex: int(dayIndex),
				PlanFile: plan.PlanFiles[0],
			})
			mu.Unlock()
		}
	})

	// Plans with higher day-index (more recent) are put at the beginning.
	sort.Slice(queue, func(i, j int) bool {
		return queue[i].DayIndex > queue[j].DayIndex
	})

	for st, c := range stats {
		s.planStatus.WithLabelValues(st.String()).Set(float64(c))
	}

	s.scanMu.Lock()
	s.allUserPlans = allPlans
	s.plansQueue = queue
	s.updateQueuedPlansMetrics()
	s.scanMu.Unlock()

	totalPlans := 0
	for _, p := range allPlans {
		totalPlans += len(p)
	}

	level.Info(s.log).Log("msg", "plans scan finished", "queued", len(queue), "total_plans", totalPlans)

	return nil
}

func (s *Scheduler) deleteObsoleteProgressFiles(ctx context.Context, plan *plan, planBaseName string) {
	for pg, t := range plan.ProgressFiles {
		if time.Since(t) < s.cfg.MaxProgressFileAge {
			continue
		}

		level.Warn(s.log).Log("msg", "found obsolete progress file, will be deleted and error uploaded", "path", pg)

		errFile := blocksconvert.ErrorFilename(planBaseName)
		if err := s.bucket.Upload(ctx, blocksconvert.ErrorFilename(planBaseName), strings.NewReader("Obsolete progress file found: "+pg)); err != nil {
			level.Error(s.log).Log("msg", "failed to create error for obsolete progress file", "err", err)
			continue
		}

		plan.ErrorFile = errFile

		if err := s.bucket.Delete(ctx, pg); err != nil {
			level.Error(s.log).Log("msg", "failed to delete obsolete progress file", "path", pg, "err", err)
			continue
		}

		delete(plan.ProgressFiles, pg)
	}
}

// Returns next plan that builder should work on.
func (s *Scheduler) NextPlan(ctx context.Context, req *blocksconvert.NextPlanRequest) (*blocksconvert.NextPlanResponse, error) {
	if s.State() != services.Running {
		return &blocksconvert.NextPlanResponse{}, nil
	}

	plan, progress := s.nextPlanNoRunningCheck(ctx)
	if plan != "" {
		level.Info(s.log).Log("msg", "sending plan file", "plan", plan, "service", req.Name)
	}
	return &blocksconvert.NextPlanResponse{
		PlanFile:     plan,
		ProgressFile: progress,
	}, nil
}

func (s *Scheduler) nextPlanNoRunningCheck(ctx context.Context) (string, string) {
	p := s.getNextPlanAndIncreaseDequeuingWG()
	if p == "" {
		return "", ""
	}

	// otherwise dequeueWG has been increased
	defer s.dequeueWG.Done()

	// Before we return plan file, we create progress file.
	ok, base := blocksconvert.IsPlanFilename(p)
	if !ok {
		// Should not happen
		level.Error(s.log).Log("msg", "enqueued file is not a plan file", "path", p)
		return "", ""
	}

	pg := blocksconvert.StartingFilename(base, time.Now())
	err := s.bucket.Upload(ctx, pg, strings.NewReader("starting"))
	if err != nil {
		level.Error(s.log).Log("msg", "failed to create progress file", "path", pg, "err", err)
		return "", ""
	}

	level.Info(s.log).Log("msg", "uploaded new progress file", "progressFile", pg)
	return p, pg
}

func (s *Scheduler) getNextPlanAndIncreaseDequeuingWG() string {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	if s.scanning {
		return ""
	}

	if len(s.plansQueue) == 0 {
		return ""
	}

	var p string
	p, s.plansQueue = s.plansQueue[0].PlanFile, s.plansQueue[1:]
	s.updateQueuedPlansMetrics()

	s.dequeueWG.Add(1)
	return p
}

func runConcurrently(ctx context.Context, concurrency int, users []string, userFunc func(user string)) {
	wg := sync.WaitGroup{}
	ch := make(chan string)

	for ix := 0; ix < concurrency; ix++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for userID := range ch {
				userFunc(userID)
			}
		}()
	}

sendLoop:
	for _, userID := range users {
		select {
		case ch <- userID:
			// ok
		case <-ctx.Done():
			// don't start new tasks.
			break sendLoop
		}
	}

	close(ch)

	// wait for ongoing workers to finish.
	wg.Wait()
}

func scanForUsers(ctx context.Context, bucket objstore.Bucket, bucketPrefix string) ([]string, error) {
	var users []string
	err := bucket.Iter(ctx, bucketPrefix, func(entry string) error {
		users = append(users, strings.TrimSuffix(entry[len(bucketPrefix)+1:], "/"))
		return nil
	})

	return users, err
}

// Returns map of "base name" -> plan. Base name is object name of the plan, with removed prefix
// and also stripped from suffixes. Scanner-produced base names are day indexes.
// Individual paths in plan struct are full paths.
func scanForPlans(ctx context.Context, bucket objstore.Bucket, prefix string) (map[string]plan, error) {
	plans := map[string]plan{}

	err := bucket.Iter(ctx, prefix, func(fullPath string) error {
		if !strings.HasPrefix(fullPath, prefix) {
			return errors.Errorf("invalid prefix: %v", fullPath)
		}

		filename := fullPath[len(prefix):]
		if ok, base := blocksconvert.IsPlanFilename(filename); ok {
			p := plans[base]
			p.PlanFiles = append(p.PlanFiles, fullPath)
			plans[base] = p
		} else if ok, base, ts := blocksconvert.IsProgressFilename(filename); ok {
			p := plans[base]
			if p.ProgressFiles == nil {
				p.ProgressFiles = map[string]time.Time{}
			}
			p.ProgressFiles[fullPath] = ts
			plans[base] = p
		} else if ok, base, id := blocksconvert.IsFinishedFilename(filename); ok {
			p := plans[base]
			p.Finished = append(p.Finished, id)
			plans[base] = p
		} else if ok, base := blocksconvert.IsErrorFilename(filename); ok {
			p := plans[base]
			p.ErrorFile = fullPath
			plans[base] = p
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return plans, nil
}

var plansTemplate = template.Must(template.New("plans").Parse(`
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Queue, Plans</title>
	</head>
	<body>
		<p>Current time: {{ .Now }}</p>
		<h1>Queue</h1>
		<ul>
		{{ range $i, $p := .Queue }}
			<li>{{ .DayIndex }} - {{ .PlanFile }}</li>
		{{ end }}
		</ul>

		<h1>Users</h1>
		{{ range $u, $up := .Plans }}
			<h2>{{ $u }}</h2>

			<table width="100%" border="1">
				<thead>
					<tr>
						<th>Plan File</th>
						<th>Status</th>
						<th>Comment</th>
					</tr>
				</thead>
				<tbody>
					{{ range $base, $planStatus := $up }}
						{{ with $planStatus }}
						<tr>
							<td>{{ range .PlanFiles }}{{ . }}<br />{{ end }}</td>
							<td>{{ .Status }}</td>
							<td>
								{{ if .ErrorFile }} <strong>Error:</strong> {{ .ErrorFile }} <br />{{ end }}
								{{ if .ProgressFiles }} <strong>Progress:</strong> {{ range $p, $t := .ProgressFiles }} {{ $p }} {{ end }} <br /> {{ end }}
								{{ if .Finished }} <strong>Finished:</strong> {{ .Finished }} <br />{{ end }}
							</td>
						</tr>
						{{ end }}
					{{ end }}
				</tbody>
			</table>
		{{ end }}
	</body>
</html>`))

func (s *Scheduler) httpPlans(writer http.ResponseWriter, req *http.Request) {
	s.scanMu.Lock()
	plans := s.allUserPlans
	queue := s.plansQueue
	s.scanMu.Unlock()

	data := struct {
		Now   time.Time
		Plans map[string]map[string]plan
		Queue []queuedPlan
	}{
		Now:   time.Now(),
		Plans: plans,
		Queue: queue,
	}

	util.RenderHTTPResponse(writer, data, plansTemplate, req)
}

// This function runs with lock.
func (s *Scheduler) updateQueuedPlansMetrics() {
	s.queuedPlansGauge.Set(float64(len(s.plansQueue)))

	if len(s.plansQueue) > 0 {
		daySeconds := 24 * time.Hour.Seconds()
		s.oldestPlanTimestamp.Set(float64(s.plansQueue[len(s.plansQueue)-1].DayIndex) * daySeconds)
		s.newestPlanTimestamp.Set(float64(s.plansQueue[0].DayIndex) * daySeconds)
	} else {
		s.oldestPlanTimestamp.Set(0)
		s.newestPlanTimestamp.Set(0)
	}
}

func (s *Scheduler) ignoreUsers(users []string) []string {
	if s.ignoredUsers == nil {
		return users
	}

	result := make([]string, 0, len(users))
	for _, u := range users {
		if !s.ignoredUsers.MatchString(u) {
			result = append(result, u)
		}
	}
	return result
}
