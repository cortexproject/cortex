package scanner

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/aws"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

type Config struct {
	TableNames  string
	TablesLimit int

	PeriodStart flagext.DayValue
	PeriodEnd   flagext.DayValue

	OutputDirectory string
	Concurrency     int

	VerifyPlans bool
	UploadFiles bool
	KeepFiles   bool

	AllowedUsers       string
	IgnoredUserPattern string
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TableNames, "scanner.tables", "", "Comma-separated tables to generate plan files from. If not used, all tables found via schema and scanning of Index store will be used.")
	f.StringVar(&cfg.OutputDirectory, "scanner.output-dir", "", "Local directory used for storing temporary plan files (will be created if missing).")
	f.IntVar(&cfg.Concurrency, "scanner.concurrency", 16, "Number of concurrent index processors, and plan uploads.")
	f.BoolVar(&cfg.UploadFiles, "scanner.upload", true, "Upload plan files.")
	f.BoolVar(&cfg.KeepFiles, "scanner.keep-files", false, "Keep plan files locally after uploading.")
	f.IntVar(&cfg.TablesLimit, "scanner.tables-limit", 0, "Number of tables to convert. 0 = all.")
	f.StringVar(&cfg.AllowedUsers, "scanner.allowed-users", "", "Allowed users that can be converted, comma-separated. If set, only these users have plan files generated.")
	f.StringVar(&cfg.IgnoredUserPattern, "scanner.ignore-users-regex", "", "If set and user ID matches this regex pattern, it will be ignored. Checked after applying -scanner.allowed-users, if set.")
	f.BoolVar(&cfg.VerifyPlans, "scanner.verify-plans", true, "Verify plans before uploading to bucket. Enabled by default for extra check. Requires extra memory for large plans.")
	f.Var(&cfg.PeriodStart, "scanner.scan-period-start", "If specified, this is lower end of time period to scan. Specified date is included in the range. (format: \"2006-01-02\")")
	f.Var(&cfg.PeriodEnd, "scanner.scan-period-end", "If specified, this is upper end of time period to scan. Specified date is not included in the range. (format: \"2006-01-02\")")
}

type Scanner struct {
	services.Service

	cfg        Config
	storageCfg storage.Config

	bucketPrefix string
	bucket       objstore.Bucket

	logger log.Logger
	reg    prometheus.Registerer

	series                        prometheus.Counter
	openFiles                     prometheus.Gauge
	indexEntries                  *prometheus.CounterVec
	indexReaderRowsRead           prometheus.Counter
	indexReaderParsedIndexEntries prometheus.Counter
	ignoredEntries                prometheus.Counter
	foundTables                   prometheus.Counter
	processedTables               prometheus.Counter
	currentTableRanges            prometheus.Gauge
	currentTableScannedRanges     prometheus.Gauge

	schema       chunk.SchemaConfig
	ignoredUsers *regexp.Regexp
	allowedUsers blocksconvert.AllowedUsers
}

func NewScanner(cfg Config, scfg blocksconvert.SharedConfig, l log.Logger, reg prometheus.Registerer) (*Scanner, error) {
	err := scfg.SchemaConfig.Load()
	if err != nil {
		return nil, errors.Wrap(err, "no table name provided, and schema failed to load")
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

	if err := os.MkdirAll(cfg.OutputDirectory, os.FileMode(0700)); err != nil {
		return nil, errors.Wrapf(err, "failed to create new output directory %s", cfg.OutputDirectory)
	}

	s := &Scanner{
		cfg:          cfg,
		schema:       scfg.SchemaConfig,
		storageCfg:   scfg.StorageConfig,
		logger:       l,
		bucket:       bucketClient,
		bucketPrefix: scfg.BucketPrefix,
		reg:          reg,
		allowedUsers: users,
		ignoredUsers: ignoredUserRegex,

		indexReaderRowsRead: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_bigtable_read_rows_total",
			Help: "Number of rows read from BigTable",
		}),
		indexReaderParsedIndexEntries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_bigtable_parsed_index_entries_total",
			Help: "Number of parsed index entries",
		}),
		currentTableRanges: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scanner_bigtable_ranges_in_current_table",
			Help: "Number of ranges to scan from current table.",
		}),
		currentTableScannedRanges: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scanner_bigtable_scanned_ranges_from_current_table",
			Help: "Number of scanned ranges from current table. Resets to 0 every time a table is getting scanned or its scan has completed.",
		}),

		series: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_scanner_series_written_total",
			Help: "Number of series written to the plan files",
		}),

		openFiles: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blocksconvert_scanner_open_files",
			Help: "Number of series written to the plan files",
		}),

		indexEntries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_scanner_scanned_index_entries_total",
			Help: "Number of various index entries scanned",
		}, []string{"type"}),
		ignoredEntries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_scanner_ignored_index_entries_total",
			Help: "Number of ignored index entries because of ignoring users.",
		}),

		foundTables: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_scanner_found_tables_total",
			Help: "Number of tables found for processing.",
		}),
		processedTables: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocksconvert_scanner_processed_tables_total",
			Help: "Number of processed tables so far.",
		}),
	}

	s.Service = services.NewBasicService(nil, s.running, nil)
	return s, nil
}

func (s *Scanner) running(ctx context.Context) error {
	allTables := []tableToProcess{}

	for ix, c := range s.schema.Configs {
		if c.Schema != "v9" && c.Schema != "v10" && c.Schema != "v11" {
			level.Warn(s.logger).Log("msg", "skipping unsupported schema version", "version", c.Schema, "schemaFrom", c.From.String())
			continue
		}

		if c.IndexTables.Period%(24*time.Hour) != 0 {
			level.Warn(s.logger).Log("msg", "skipping invalid index table period", "period", c.IndexTables.Period, "schemaFrom", c.From.String())
			continue
		}

		var reader chunk.IndexReader
		switch c.IndexType {
		case "gcp", "gcp-columnkey", "bigtable", "bigtable-hashed":
			bigTable := s.storageCfg.GCPStorageConfig

			if bigTable.Project == "" || bigTable.Instance == "" {
				level.Error(s.logger).Log("msg", "cannot scan BigTable, missing configuration", "schemaFrom", c.From.String())
				continue
			}

			reader = newBigtableIndexReader(bigTable.Project, bigTable.Instance, s.logger, s.indexReaderRowsRead, s.indexReaderParsedIndexEntries, s.currentTableRanges, s.currentTableScannedRanges)
		case "aws-dynamo":
			cfg := s.storageCfg.AWSStorageConfig

			if cfg.DynamoDB.URL == nil {
				level.Error(s.logger).Log("msg", "cannot scan DynamoDB, missing configuration", "schemaFrom", c.From.String())
				continue
			}

			var err error
			reader, err = aws.NewDynamoDBIndexReader(cfg.DynamoDBConfig, s.schema, s.reg, s.logger, s.indexReaderRowsRead)
			if err != nil {
				level.Error(s.logger).Log("msg", "cannot scan DynamoDB", "err", err)
			}
		case "cassandra":
			cass := s.storageCfg.CassandraStorageConfig

			reader = newCassandraIndexReader(cass, s.schema, s.logger, s.indexReaderRowsRead, s.indexReaderParsedIndexEntries, s.currentTableRanges, s.currentTableScannedRanges)
		default:
			level.Warn(s.logger).Log("msg", "unsupported index type", "type", c.IndexType, "schemaFrom", c.From.String())
			continue
		}

		toTimestamp := time.Now().Add(24 * time.Hour).Truncate(24 * time.Hour).Unix()
		if ix < len(s.schema.Configs)-1 {
			toTimestamp = s.schema.Configs[ix+1].From.Unix()
		}

		level.Info(s.logger).Log("msg", "scanning for schema tables", "schemaFrom", c.From.String(), "prefix", c.IndexTables.Prefix, "period", c.IndexTables.Period)
		tables, err := s.findTablesToProcess(ctx, reader, c.From.Unix(), toTimestamp, c.IndexTables)
		if err != nil {
			return errors.Wrapf(err, "finding tables for schema %s", c.From.String())
		}

		level.Info(s.logger).Log("msg", "found tables", "count", len(tables))
		allTables = append(allTables, tables...)
	}

	level.Info(s.logger).Log("msg", "total found tables", "count", len(allTables))

	if s.cfg.TableNames != "" {
		// Find tables from parameter.
		tableNames := map[string]bool{}
		for _, t := range strings.Split(s.cfg.TableNames, ",") {
			tableNames[strings.TrimSpace(t)] = true
		}

		for ix := 0; ix < len(allTables); {
			t := allTables[ix]
			if !tableNames[t.table] {
				// remove table.
				allTables = append(allTables[:ix], allTables[ix+1:]...)
				continue
			}
			ix++
		}

		level.Error(s.logger).Log("msg", "applied tables filter", "selected", len(allTables))
	}

	// Recent tables go first.
	sort.Slice(allTables, func(i, j int) bool {
		return allTables[i].start.After(allTables[j].start)
	})

	for ix := 0; ix < len(allTables); {
		t := allTables[ix]
		if s.cfg.PeriodStart.IsSet() && !t.end.IsZero() && t.end.Unix() <= s.cfg.PeriodStart.Unix() {
			level.Info(s.logger).Log("msg", "table ends before period-start, ignoring", "table", t.table, "table_start", t.start.String(), "table_end", t.end.String(), "period_start", s.cfg.PeriodStart.String())
			allTables = append(allTables[:ix], allTables[ix+1:]...)
			continue
		}
		if s.cfg.PeriodEnd.IsSet() && t.start.Unix() >= s.cfg.PeriodEnd.Unix() {
			level.Info(s.logger).Log("msg", "table starts after period-end, ignoring", "table", t.table, "table_start", t.start.String(), "table_end", t.end.String(), "period_end", s.cfg.PeriodEnd.String())
			allTables = append(allTables[:ix], allTables[ix+1:]...)
			continue
		}
		ix++
	}

	if s.cfg.TablesLimit > 0 && len(allTables) > s.cfg.TablesLimit {
		level.Info(s.logger).Log("msg", "applied tables limit", "limit", s.cfg.TablesLimit)
		allTables = allTables[:s.cfg.TablesLimit]
	}

	s.foundTables.Add(float64(len(allTables)))

	for _, t := range allTables {
		if err := s.processTable(ctx, t.table, t.reader); err != nil {
			return errors.Wrapf(err, "failed to process table %s", t.table)
		}
		s.processedTables.Inc()
	}

	// All good, just wait until context is done, to avoid restarts.
	level.Info(s.logger).Log("msg", "finished")
	<-ctx.Done()
	return nil
}

type tableToProcess struct {
	table  string
	reader chunk.IndexReader
	start  time.Time
	end    time.Time // Will not be set for non-periodic tables. Exclusive.
}

func (s *Scanner) findTablesToProcess(ctx context.Context, indexReader chunk.IndexReader, fromUnixTimestamp, toUnixTimestamp int64, tablesConfig chunk.PeriodicTableConfig) ([]tableToProcess, error) {
	tables, err := indexReader.IndexTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var result []tableToProcess

	for _, t := range tables {
		if !strings.HasPrefix(t, tablesConfig.Prefix) {
			continue
		}

		var tp tableToProcess
		if tablesConfig.Period == 0 {
			tp = tableToProcess{
				table:  t,
				reader: indexReader,
				start:  time.Unix(fromUnixTimestamp, 0),
			}
		} else {
			p, err := strconv.ParseInt(t[len(tablesConfig.Prefix):], 10, 64)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed to parse period index of table", "table", t)
				continue
			}

			start := time.Unix(p*int64(tablesConfig.Period/time.Second), 0)
			tp = tableToProcess{
				table:  t,
				reader: indexReader,
				start:  start,
				end:    start.Add(tablesConfig.Period),
			}
		}

		if fromUnixTimestamp <= tp.start.Unix() && tp.start.Unix() < toUnixTimestamp {
			result = append(result, tp)
		}
	}

	return result, nil
}

func (s *Scanner) processTable(ctx context.Context, table string, indexReader chunk.IndexReader) error {
	tableLog := log.With(s.logger, "table", table)

	tableProcessedFile := filepath.Join(s.cfg.OutputDirectory, table+".processed")

	if shouldSkipOperationBecauseFileExists(tableProcessedFile) {
		level.Info(tableLog).Log("msg", "skipping table because it was already scanned")
		return nil
	}

	dir := filepath.Join(s.cfg.OutputDirectory, table)
	level.Info(tableLog).Log("msg", "scanning table", "output", dir)

	ignoredUsers, err := scanSingleTable(ctx, indexReader, table, dir, s.cfg.Concurrency, s.allowedUsers, s.ignoredUsers, s.openFiles, s.series, s.indexEntries, s.ignoredEntries)
	if err != nil {
		return errors.Wrapf(err, "failed to scan table %s and generate plan files", table)
	}

	tableLog.Log("msg", "ignored users", "count", len(ignoredUsers), "users", strings.Join(ignoredUsers, ","))

	if s.cfg.VerifyPlans {
		err = verifyPlanFiles(ctx, dir, tableLog)
		if err != nil {
			return errors.Wrap(err, "failed to verify plans")
		}
	}

	if s.bucket != nil {
		level.Info(tableLog).Log("msg", "uploading generated plan files for table", "source", dir)

		err := uploadPlansConcurrently(ctx, tableLog, dir, s.bucket, s.bucketPrefix, s.cfg.Concurrency)
		if err != nil {
			return errors.Wrapf(err, "failed to upload plan files for table %s to bucket", table)
		}

		level.Info(tableLog).Log("msg", "uploaded generated files for table")
		if !s.cfg.KeepFiles {
			if err := os.RemoveAll(dir); err != nil {
				return errors.Wrapf(err, "failed to delete uploaded plan files for table %s", table)
			}
		}
	}

	err = ioutil.WriteFile(tableProcessedFile, []byte("Finished on "+time.Now().String()+"\n"), 0600)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", tableProcessedFile)
	}

	level.Info(tableLog).Log("msg", "done processing table")
	return nil
}

func uploadPlansConcurrently(ctx context.Context, log log.Logger, dir string, bucket objstore.Bucket, bucketPrefix string, concurrency int) error {
	df, err := os.Stat(dir)
	if err != nil {
		return errors.Wrap(err, "stat dir")
	}
	if !df.IsDir() {
		return errors.Errorf("%s is not a directory", dir)
	}

	// Path relative to dir, and only use Slash as separator. BucketPrefix is prepended to it when uploading.
	paths := make(chan string)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for p := range paths {
				src := filepath.Join(dir, filepath.FromSlash(p))
				dst := path.Join(bucketPrefix, p)

				boff := backoff.New(ctx, backoff.Config{
					MinBackoff: 1 * time.Second,
					MaxBackoff: 5 * time.Second,
					MaxRetries: 5,
				})

				for boff.Ongoing() {
					err := objstore.UploadFile(ctx, log, bucket, src, dst)

					if err == nil {
						break
					}

					level.Warn(log).Log("msg", "failed to upload block", "err", err)
					boff.Wait()
				}

				if boff.Err() != nil {
					return boff.Err()
				}
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(paths)

		return filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if fi.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}

			relPath = filepath.ToSlash(relPath)

			select {
			case paths <- relPath:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	})

	return g.Wait()
}

func shouldSkipOperationBecauseFileExists(file string) bool {
	// If file exists, we should skip the operation.
	_, err := os.Stat(file)
	// Any error (including ErrNotExists) indicates operation should continue.
	return err == nil
}

func scanSingleTable(
	ctx context.Context,
	indexReader chunk.IndexReader,
	tableName string,
	outDir string,
	concurrency int,
	allowed blocksconvert.AllowedUsers,
	ignored *regexp.Regexp,
	openFiles prometheus.Gauge,
	series prometheus.Counter,
	indexEntries *prometheus.CounterVec,
	ignoredEntries prometheus.Counter,
) ([]string, error) {
	err := os.RemoveAll(outDir)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to delete directory %s", outDir)
	}

	err = os.MkdirAll(outDir, os.FileMode(0700))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to prepare directory %s", outDir)
	}

	files := newOpenFiles(openFiles)
	result := func(dir string, file string, entry blocksconvert.PlanEntry, header func() blocksconvert.PlanEntry) error {
		return files.appendJSONEntryToFile(dir, file, entry, func() interface{} {
			return header()
		})
	}

	var ps []chunk.IndexEntryProcessor

	for i := 0; i < concurrency; i++ {
		ps = append(ps, newProcessor(outDir, result, allowed, ignored, series, indexEntries, ignoredEntries))
	}

	err = indexReader.ReadIndexEntries(ctx, tableName, ps)
	if err != nil {
		return nil, err
	}

	ignoredUsersMap := map[string]struct{}{}
	for _, p := range ps {
		for u := range p.(*processor).ignoredUsers {
			ignoredUsersMap[u] = struct{}{}
		}
	}

	var ignoredUsers []string
	for u := range ignoredUsersMap {
		ignoredUsers = append(ignoredUsers, u)
	}

	err = files.closeAllFiles(func() interface{} {
		return blocksconvert.PlanEntry{Complete: true}
	})
	return ignoredUsers, errors.Wrap(err, "closing files")
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

		ok, _ := blocksconvert.IsPlanFilename(info.Name())
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
