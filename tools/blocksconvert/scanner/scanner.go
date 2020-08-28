package scanner

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type Config struct {
	TableName   string
	TablesLimit int

	OutputDirectory string
	Concurrency     int

	UploadFiles bool
	KeepFiles   bool

	IgnoredUserPattern string
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TableName, "table", "", "Table to generate plan files from. If not used, tables are discovered via schema.")
	f.StringVar(&cfg.OutputDirectory, "scanner.local-dir", "", "Local directory used for storing temporary plan files (will be deleted and recreated!).")
	f.IntVar(&cfg.Concurrency, "scanner.concurrency", 16, "Number of concurrent index processors.")
	f.BoolVar(&cfg.UploadFiles, "scanner.upload", true, "Upload plan files.")
	f.BoolVar(&cfg.KeepFiles, "scanner.keep-files", false, "Keep plan files locally after uploading.")
	f.IntVar(&cfg.TablesLimit, "scanner.tables-limit", 0, "Number of tables to convert. 0 = all.")
	f.StringVar(&cfg.IgnoredUserPattern, "scanner.ignore-user", "", "Regex pattern with ignored users.")
}

type Scanner struct {
	services.Service

	cfg          Config
	bucketPrefix string
	indexReader  IndexReader

	series       prometheus.Counter
	openFiles    prometheus.Gauge
	indexEntries *prometheus.CounterVec
	logger       log.Logger

	tablePeriod time.Duration

	table       string
	tablePrefix string
	bucket      objstore.Bucket
	ignored     *regexp.Regexp
}

func NewScanner(cfg Config, scfg blocksconvert.SharedConfig, l log.Logger, reg prometheus.Registerer) (*Scanner, error) {
	bigTable := scfg.StorageConfig.GCPStorageConfig

	if bigTable.Project == "" || bigTable.Instance == "" {
		return nil, errors.New("missing BigTable configuration")
	}

	tablePrefix := ""
	tablePeriod := time.Duration(0)
	if cfg.TableName == "" {
		err := scfg.SchemaConfig.Load()
		if err != nil {
			return nil, errors.Wrap(err, "no table name provided, and schema failed to load")
		}

		for _, c := range scfg.SchemaConfig.Configs {
			if c.IndexTables.Period%(24*time.Hour) != 0 {
				return nil, errors.Errorf("invalid index table period: %v", c.IndexTables.Period)
			}

			if c.Schema != "v9" && c.Schema != "v10" && c.Schema != "v11" {
				return nil, errors.Errorf("unsupported schema version: %v", c.Schema)
			}

			if tablePrefix == "" {
				tablePrefix = c.IndexTables.Prefix
				tablePeriod = c.IndexTables.Period
			} else if tablePrefix != c.IndexTables.Prefix {
				return nil, errors.Errorf("multiple index table prefixes found in schema: %v, %v", tablePrefix, c.IndexTables.Prefix)
			} else if tablePeriod != c.IndexTables.Period {
				return nil, errors.Errorf("multiple index table periods found in schema: %v, %v", tablePeriod, c.IndexTables.Period)
			}
		}
	}

	if cfg.OutputDirectory == "" {
		return nil, errors.Errorf("no output directory")
	}

	var bucketClient objstore.Bucket
	if cfg.UploadFiles {
		var err error
		bucketClient, err = scfg.GetBucket(l, reg)
		if err != nil {
			return nil, err
		}
	}

	var ignoredUserRegex *regexp.Regexp = nil
	if cfg.IgnoredUserPattern != "" {
		re, err := regexp.Compile(cfg.IgnoredUserPattern)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile ignored user regex")
		}
		ignoredUserRegex = re
	}

	err := os.MkdirAll(cfg.OutputDirectory, os.FileMode(0700))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new output directory %s", cfg.OutputDirectory)
	}

	s := &Scanner{
		cfg:          cfg,
		indexReader:  newBigtableIndexReader(bigTable.Project, bigTable.Instance, l, reg),
		table:        cfg.TableName,
		tablePrefix:  tablePrefix,
		tablePeriod:  tablePeriod,
		logger:       l,
		bucket:       bucketClient,
		bucketPrefix: scfg.BucketPrefix,

		ignored: ignoredUserRegex,

		series: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "scanner_series_written_total",
			Help: "Number of series written to the plan files",
		}),

		openFiles: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "scanner_open_files",
			Help: "Number of series written to the plan files",
		}),

		indexEntries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "scanner_scanned_index_entries_total",
			Help: "Number of various index entries scanned",
		}, []string{"type"}),
	}

	s.Service = services.NewBasicService(nil, s.running, nil)
	return s, nil
}

func (s *Scanner) running(ctx context.Context) error {
	var tables []string
	if s.table == "" {
		// Use table prefix to discover tables to scan.
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

		err := scanSingleTable(ctx, s.indexReader, t, dir, s.cfg.Concurrency, s.openFiles, s.series, s.ignored, s.indexEntries)
		if err != nil {
			return errors.Wrapf(err, "failed to scan table %s and generate plan files", t)
		}

		err = verifyPlanFiles(ctx, dir, s.logger)
		if err != nil {
			return errors.Wrap(err, "failed to verify plans")
		}

		if s.bucket != nil {
			level.Info(s.logger).Log("msg", "uploading generated plan files for table", "table", t, "source", dir)

			err := objstore.UploadDir(ctx, s.logger, s.bucket, dir, s.bucketPrefix)
			if err != nil {
				return errors.Wrapf(err, "failed to upload plan files for table %s to bucket", t)
			}

			level.Info(s.logger).Log("msg", "uploaded generated files for table", "table", t)
			if !s.cfg.KeepFiles {
				if err := os.RemoveAll(dir); err != nil {
					return errors.Wrapf(err, "failed to delete uploaded plan files for table %s", t)
				}
			}
		}

		err = ioutil.WriteFile(tableProcessedFile, []byte("Finished on "+time.Now().String()+"\n"), 0600)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %s", tableProcessedFile)
		}

		level.Info(s.logger).Log("msg", "done scanning table", "table", t)
	}

	// All good, just wait until context is done, to avoid restarts.
	level.Info(s.logger).Log("msg", "finished")
	<-ctx.Done()
	return nil
}

func shouldSkipOperationBecauseFileExists(file string) bool {
	// If file exists, we should skip the operation.
	_, err := os.Stat(file)
	// Any error (including ErrNotExists) indicates operation should continue.
	return err == nil
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

func scanSingleTable(ctx context.Context, indexReader IndexReader, tableName string, outDir string, concurrency int, openFiles prometheus.Gauge, series prometheus.Counter, ignored *regexp.Regexp, indexEntries *prometheus.CounterVec) error {
	err := os.RemoveAll(outDir)
	if err != nil {
		return errors.Wrapf(err, "failed to delete directory %s", outDir)
	}

	err = os.MkdirAll(outDir, os.FileMode(0700))
	if err != nil {
		return errors.Wrapf(err, "failed to prepare directory %s", outDir)
	}

	files := newOpenFiles(openFiles)
	result := func(dir string, file string, entry blocksconvert.PlanEntry, header func() blocksconvert.PlanEntry) error {
		return files.appendJSONEntryToFile(dir, file, entry, func() interface{} {
			return header()
		})
	}

	var ps []IndexEntryProcessor

	for i := 0; i < concurrency; i++ {
		ps = append(ps, newProcessor(outDir, result, ignored, series, indexEntries))
	}

	err = indexReader.ReadIndexEntries(ctx, tableName, ps)
	if err != nil {
		return err
	}

	err = files.closeAllFiles(func() interface{} {
		return blocksconvert.PlanEntry{Complete: true}
	})
	return errors.Wrap(err, "closing files")
}

func verifyPlanFiles(ctx context.Context, dir string, logger log.Logger) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		ok, _ := blocksconvert.IsPlanFile(info.Name())
		if !ok {
			return nil
		}

		r, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open %s", path)
		}
		defer func() {
			_ = r.Close()
		}()

		pr, err := blocksconvert.PreparePlanFileReader(info.Name(), r)
		if err != nil {
			return errors.Wrapf(err, "failed to prepare plan file for reading: %s", path)
		}

		level.Info(logger).Log("msg", "verifying plan", "path", path)
		return errors.Wrapf(verifyPlanFile(pr), "plan file: %s", path)
	})
}

func verifyPlanFile(r io.Reader) error {
	dec := json.NewDecoder(r)

	entry := blocksconvert.PlanEntry{}
	if err := dec.Decode(&entry); err != nil {
		return errors.Wrap(err, "failed to parse plan file header")
	}
	if entry.User == "" || entry.DayIndex == 0 {
		return errors.New("failed to read plan file header: no user or day index found")
	}

	series := map[string]struct{}{}

	var err error
	footerFound := false
	for err = dec.Decode(&entry); err == nil; err = dec.Decode(&entry) {
		if entry.Complete {
			footerFound = true
			entry.Reset()
			continue
		}

		if footerFound {
			return errors.New("plan entries found after plan footer")
		}

		if entry.SeriesID == "" {
			return errors.Errorf("plan contains entry without seriesID")
		}

		if len(entry.Chunks) == 0 {
			return errors.Errorf("entry for seriesID %s has no chunks", entry.SeriesID)
		}

		if _, found := series[entry.SeriesID]; found {
			return errors.Errorf("multiple entries for series %s found in plan", entry.SeriesID)
		}
		series[entry.SeriesID] = struct{}{}

		entry.Reset()
	}

	if err == io.EOF {
		if !footerFound {
			return errors.New("no footer found in the plan")
		}
		err = nil
	}
	return err
}
