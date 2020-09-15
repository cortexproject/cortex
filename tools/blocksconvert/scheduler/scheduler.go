package scheduler

import (
	"context"
	"flag"
	"html/template"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
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
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.ScanInterval, "scheduler.scan-interval", 5*time.Minute, "How often to scan for plans and their status.")
	f.IntVar(&cfg.PlanScanConcurrency, "scheduler.plan-scan-concurrency", 5, "Limit of concurrent plan scans.")
	f.DurationVar(&cfg.MaxProgressFileAge, "scheduler.max-progress-file-age", 30*time.Minute, "Progress files older than this duration are deleted.")
	f.StringVar(&cfg.AllowedUsers, "scheduler.allowed-users", "", "Allowed users that can be converted, comma-separated")
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

	s := newSchedulerWithBucket(l, b, scfg.BucketPrefix, users, cfg, reg)
	blocksconvert.RegisterSchedulerServer(grpcServ, s)
	http.HandleFunc("/plans", s.httpPlans)
	return s, nil
}

func newSchedulerWithBucket(l log.Logger, b objstore.Bucket, bucketPrefix string, users blocksconvert.AllowedUsers, cfg Config, reg prometheus.Registerer) *Scheduler {
	s := &Scheduler{
		log:          l,
		cfg:          cfg,
		bucket:       b,
		bucketPrefix: bucketPrefix,
		allowedUsers: users,

		planStatus: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scheduler_scanned_plans",
			Help: "Number of plans in different status",
		}, []string{"status"}),
	}

	s.Service = services.NewTimerService(cfg.ScanInterval, s.scanBucketForPlans, s.scanBucketForPlans, nil)
	return s
}

type Scheduler struct {
	services.Service
	cfg Config
	log log.Logger

	allowedUsers blocksconvert.AllowedUsers

	bucket       objstore.Bucket
	bucketPrefix string

	planStatus *prometheus.GaugeVec

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

	level.Info(s.log).Log("msg", "found users", "all", allUsers, "allowed", len(users))

	var mu sync.Mutex
	allPlans := map[string]map[string]plan{}
	stats := map[planStatus]int{}
	for _, k := range []planStatus{New, InProgress, Finished, Error, Invalid} {
		stats[k] = 0
	}
	var queue []queuedPlan

	runConcurrently(ctx, s.cfg.PlanScanConcurrency, users, func(user string) {
		userPlans, err := scanForPlans(ctx, s.bucket, s.bucketPrefix, user)
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
				for pg, t := range plan.ProgressFiles {
					if time.Since(t) > s.cfg.MaxProgressFileAge {
						level.Warn(s.log).Log("msg", "deleting obsolete progress file", "path", pg)
						err := s.bucket.Delete(ctx, pg)
						if err != nil {
							level.Error(s.log).Log("msg", "failed to delete obsolete progress file", "path", pg, "err", err)
						} else {
							delete(plan.ProgressFiles, pg)
						}
					}
				}

				// After deleting old progress files, status might have changed from InProgress to New.
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
	s.scanMu.Unlock()

	totalPlans := 0
	for _, p := range allPlans {
		totalPlans += len(p)
	}

	level.Info(s.log).Log("msg", "plans scan finished", "queued", len(queue), "total_plans", totalPlans)

	return nil
}

// Returns next plan that builder should work on.
func (s *Scheduler) NextPlan(ctx context.Context, req *blocksconvert.NextPlanRequest) (*blocksconvert.NextPlanResponse, error) {
	if s.State() != services.Running {
		return &blocksconvert.NextPlanResponse{}, nil
	}

	plan, progress := s.nextPlanNoRunningCheck(ctx)
	if plan != "" {
		level.Info(s.log).Log("msg", "sending plan file to builder", "plan", plan, "builder", req.BuilderName)
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

func scanForPlans(ctx context.Context, bucket objstore.Bucket, bucketPrefix, user string) (map[string]plan, error) {
	prefix := path.Join(bucketPrefix, user)
	prefixWithSlash := prefix + "/"

	plans := map[string]plan{}

	err := bucket.Iter(ctx, prefix, func(fullPath string) error {
		if !strings.HasPrefix(fullPath, prefixWithSlash) {
			return errors.Errorf("invalid prefix: %v", fullPath)
		}

		filename := fullPath[len(prefixWithSlash):]
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
			p.Blocks = append(p.Blocks, id)
			plans[base] = p
		} else if ok, base := blocksconvert.IsErrorFilename(filename); ok {
			p := plans[base]
			p.ErrorFile = fullPath
			plans[base] = p
		}

		return nil
	})

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get plan status for user %s", user)
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
								{{ if .Blocks }} <strong>Blocks:</strong> {{ .Blocks }} <br />{{ end }}
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
