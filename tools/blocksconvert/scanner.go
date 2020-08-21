package blocksconvert

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type ScannerConfig struct {
	BigtableProject  string
	BigtableInstance string

	TableName    string
	SchemaConfig chunk.SchemaConfig
	TablesLimit  int

	OutputDirectory string
	Concurrency     int

	UploadFiles  bool
	Bucket       tsdb.BucketConfig
	BucketPrefix string
	KeepFiles    bool
}

func (cfg *ScannerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(flag.CommandLine)
	cfg.Bucket.RegisterFlags(flag.CommandLine)

	f.StringVar(&cfg.BigtableProject, "bigtable.project", "", "The Google Cloud Platform project ID. Required.")
	f.StringVar(&cfg.BigtableInstance, "bigtable.instance", "", "The Google Cloud Bigtable instance ID. Required.")
	f.StringVar(&cfg.TableName, "table", "", "Table to generate plan files from. If not used, tables are discovered via schema.")
	f.StringVar(&cfg.OutputDirectory, "scanner.local-dir", "", "Local directory used for storing temporary plan files (will be deleted and recreated!).")
	f.IntVar(&cfg.Concurrency, "scanner.concurrency", 16, "Number of concurrent index processors.")
	f.BoolVar(&cfg.UploadFiles, "scanner.upload", true, "Upload plan files.")
	f.BoolVar(&cfg.KeepFiles, "scanner.keep-files", false, "Keep plan files locally after uploading.")
	f.StringVar(&cfg.BucketPrefix, "workspace.prefix", "migration", "Prefix in the bucket for storing plan files.")
	f.IntVar(&cfg.TablesLimit, "scanner.tables-limit", 0, "Number of tables to convert. 0 = all.")
}

type Scanner struct {
	services.Service

	cfg         ScannerConfig
	indexReader IndexReader

	series    prometheus.Counter
	openFiles prometheus.Gauge
	logger    log.Logger

	tablePeriod time.Duration

	table       string
	tablePrefix string
	bucket      objstore.Bucket
}

func NewScanner(cfg ScannerConfig, l log.Logger, reg prometheus.Registerer) (*Scanner, error) {
	if cfg.BigtableProject == "" || cfg.BigtableInstance == "" {
		return nil, fmt.Errorf("missing BigTable configuration")
	}

	tablePrefix := ""
	tablePeriod := time.Duration(0)
	if cfg.TableName == "" {
		err := cfg.SchemaConfig.Load()
		if err != nil {
			return nil, fmt.Errorf("no table name provided, and schema failed to load: %w", err)
		}

		for _, c := range cfg.SchemaConfig.Configs {
			if c.IndexTables.Period%(24*time.Hour) != 0 {
				return nil, fmt.Errorf("invalid index table period: %v", c.IndexTables.Period)
			}

			if c.Schema != "v9" && c.Schema != "v10" && c.Schema != "v11" {
				return nil, fmt.Errorf("unsupported schema version: %v", c.Schema)
			}

			if tablePrefix == "" {
				tablePrefix = c.IndexTables.Prefix
				tablePeriod = c.IndexTables.Period
			} else if tablePrefix != c.IndexTables.Prefix {
				return nil, fmt.Errorf("multiple index table prefixes found in schema: %v, %v", tablePrefix, c.IndexTables.Prefix)
			} else if tablePeriod != c.IndexTables.Period {
				return nil, fmt.Errorf("multiple index table periods found in schema: %v, %v", tablePeriod, c.IndexTables.Period)
			}
		}
	}

	if cfg.OutputDirectory == "" {
		return nil, fmt.Errorf("no output directory")
	}

	var bucketClient objstore.Bucket
	if cfg.UploadFiles {
		if err := cfg.Bucket.Validate(); err != nil {
			return nil, fmt.Errorf("invalid bucket config: %w", err)
		}

		bucket, err := tsdb.NewBucketClient(context.Background(), cfg.Bucket, "bucket", l, reg)
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}

		bucketClient = bucket
	}

	err := os.MkdirAll(cfg.OutputDirectory, os.FileMode(0700))
	if err != nil {
		return nil, fmt.Errorf("failed to create new output directory %s: %w", cfg.OutputDirectory, err)
	}

	s := &Scanner{
		cfg:         cfg,
		indexReader: NewBigtableIndexReader(cfg.BigtableProject, cfg.BigtableInstance, l, reg),
		table:       cfg.TableName,
		tablePrefix: tablePrefix,
		tablePeriod: tablePeriod,
		logger:      l,
		bucket:      bucketClient,

		series: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "scanner_series_written_total",
			Help: "Number of series written to the plan files",
		}),

		openFiles: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "scanner_open_files",
			Help: "Number of series written to the plan files",
		}),
	}

	s.Service = services.NewBasicService(nil, s.running, nil)
	return s, nil
}

func (s *Scanner) running(ctx context.Context) error {
	var tables []string
	if s.table == "" {
		// Use table prefix to discover tables to scan.
		// TODO: use min/max day
		tableNames, err := s.indexReader.IndexTableNames(ctx)
		if err != nil {
			return err
		}

		tables = findTables(s.logger, tableNames, s.tablePrefix, s.tablePeriod)
		sort.Sort(sort.Reverse(sort.StringSlice(tables)))

		level.Info(s.logger).Log("msg", fmt.Sprintf("found %d tables to scan", len(tables)), "prefix", s.tablePrefix, "period", s.tablePeriod)
		if s.cfg.TablesLimit > 0 && len(tables) > s.cfg.TablesLimit {
			level.Info(s.logger).Log("msg", "applied tables limit", "limit", s.cfg.TablesLimit)
			tables = tables[:s.cfg.TablesLimit]
		}
	} else {
		tables = []string{s.table}
	}

	for _, t := range tables {
		tableProcessedFile := filepath.Join(s.cfg.OutputDirectory, t+".processed")

		if shouldSkipOperationBecauseFileExists(tableProcessedFile) {
			level.Info(s.logger).Log("msg", "skipping table because it was already scanned", "table", t)
			continue
		}

		dir := filepath.Join(s.cfg.OutputDirectory, t)
		level.Info(s.logger).Log("msg", "scanning table", "table", t, "output", dir)

		err := scanSingleTable(ctx, s.indexReader, t, dir, s.cfg.Concurrency, s.openFiles, s.series)
		if err != nil {
			return fmt.Errorf("failed to scan table %s and generate plan files: %w", t, err)
		}

		if s.bucket != nil {
			level.Info(s.logger).Log("msg", "uploading generated plan files for table", "table", t, "source", dir)

			err := objstore.UploadDir(ctx, s.logger, s.bucket, dir, s.cfg.BucketPrefix)
			if err != nil {
				return fmt.Errorf("failed to upload plan files for table %s to bucket: %w", t, err)
			}

			level.Info(s.logger).Log("msg", "uploaded generated files for table", "table", t)
			if !s.cfg.KeepFiles {
				if err := os.RemoveAll(dir); err != nil {
					return fmt.Errorf("failed to delete uploaded plan files for table %s: %w", t, err)
				}
			}
		}

		err = ioutil.WriteFile(tableProcessedFile, []byte("Finished on "+time.Now().String()), 0600)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", tableProcessedFile, err)
		}

		level.Info(s.logger).Log("msg", "done scanning table", "table", t)
	}

	// All good, just wait until context is done, to avoid restarts.
	level.Info(s.logger).Log("Finished")
	<-ctx.Done()
	return nil
}

func shouldSkipOperationBecauseFileExists(file string) bool {
	// If file exists, we should skip the operation.
	_, err := os.Stat(file)
	if err != nil {
		// Any error (including ErrNotExists) indicates operation should continue.
		return false
	}

	return true
}

func findTables(logger log.Logger, tableNames []string, prefix string, period time.Duration) []string {
	type table struct {
		name        string
		periodIndex int64
	}

	var tables []table

	for _, t := range tableNames {
		if !strings.HasPrefix(t, prefix) {
			continue
		}

		if period == 0 {
			tables = append(tables, table{
				name:        t,
				periodIndex: 0,
			})
			continue
		}

		p, err := strconv.ParseInt(t[len(prefix):], 10, 64)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to parse period index of table", "table", t)
			continue
		}

		tables = append(tables, table{
			name:        t,
			periodIndex: p,
		})
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].periodIndex < tables[j].periodIndex
	})

	var out []string
	for _, t := range tables {
		out = append(out, t.name)
	}

	return out
}

func scanSingleTable(ctx context.Context, indexReader IndexReader, tableName string, outDir string, concurrency int, openFiles prometheus.Gauge, series prometheus.Counter) error {
	err := os.RemoveAll(outDir)
	if err != nil {
		return fmt.Errorf("failed to delete directory %s: %w", outDir, err)
	}

	err = os.MkdirAll(outDir, os.FileMode(0700))
	if err != nil {
		return fmt.Errorf("failed to prepare directory %s: %w", outDir, err)
	}

	files := newOpenFiles(openFiles)

	var ps []IndexEntryProcessor

	for i := 0; i < concurrency; i++ {
		ps = append(ps, newProcessor(outDir, files, series))
	}

	err = indexReader.ReadIndexEntries(ctx, tableName, ps)
	if err != nil {
		return err
	}

	errs := files.closeAllFiles(func() interface{} {
		return PlanFooter{Complete: true}
	})
	if len(errs) > 0 {
		return errors.MultiError(errs)
	}

	return nil
}
