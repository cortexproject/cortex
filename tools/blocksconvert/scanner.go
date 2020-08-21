package blocksconvert

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type ScannerConfig struct {
	BigtableProject  string
	BigtableInstance string

	TableName       string
	OutputDirectory string
	Concurrency     int

	SchemaConfig chunk.SchemaConfig
}

func (cfg *ScannerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(flag.CommandLine)

	f.StringVar(&cfg.BigtableProject, "bigtable.project", "", "The Google Cloud Platform project ID. Required.")
	f.StringVar(&cfg.BigtableInstance, "bigtable.instance", "", "The Google Cloud Bigtable instance ID. Required.")
	f.StringVar(&cfg.TableName, "table", "", "Table to generate plan files from. If not used, tables are discovered via schema.")
	f.StringVar(&cfg.OutputDirectory, "scanner.local-dir", "", "Local directory used for storing temporary plan files (will be deleted and recreated!).")
	f.IntVar(&cfg.Concurrency, "scanner.concurrency", 16, "Number of concurrent index processors.")
}

type Scanner struct {
	services.Service

	cfg         ScannerConfig
	indexReader IndexReader

	series    prometheus.Counter
	openFiles prometheus.Gauge
	logger    log.Logger

	table       string
	tablePrefix string
}

func NewScanner(cfg ScannerConfig, l log.Logger, reg prometheus.Registerer) (*Scanner, error) {
	if cfg.BigtableProject == "" || cfg.BigtableInstance == "" {
		return nil, fmt.Errorf("missing BigTable configuration")
	}

	tablePrefix := ""
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
			} else if tablePrefix != c.IndexTables.Prefix {
				return nil, fmt.Errorf("multiple index table prefixes found in schema: %v, %v", tablePrefix, c.IndexTables.Prefix)
			}
		}
	}

	if cfg.OutputDirectory == "" {
		return nil, fmt.Errorf("no output directory")
	}

	err := os.RemoveAll(cfg.OutputDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to delete existing output directory %s: %w", cfg.OutputDirectory, err)
	}

	err = os.Mkdir(cfg.OutputDirectory, os.FileMode(0700))
	if err != nil {
		return nil, fmt.Errorf("failed to create new output directory %s: %w", cfg.OutputDirectory, err)
	}

	s := &Scanner{
		cfg:         cfg,
		indexReader: NewBigtableIndexReader(cfg.BigtableProject, cfg.BigtableInstance, l, reg),
		table:       cfg.TableName,
		tablePrefix: tablePrefix,
		logger:      l,

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
	tables := []string{}
	if s.table == "" {
		// Use table prefix to discover tables to scan.
		// TODO: use min/max day
		t, err := findTables(ctx, s.indexReader, s.tablePrefix)
		if err != nil {
			return err
		}

		s.logger.Log("msg", fmt.Sprintf("found %d tables to scan", len(t)), "prefix", s.tablePrefix)
		tables = t
	} else {
		tables = []string{s.table}
	}

	for _, t := range tables {
		// TODO: check if it was processed before.

		s.logger.Log("msg", "scanning table", "table", t)
		err := scanSingleTable(ctx, s.indexReader, t, s.cfg.OutputDirectory, s.cfg.Concurrency, s.openFiles, s.series)
		if err != nil {
			return fmt.Errorf("failed to process table %s: %w", t, err)
		}

		// TODO: upload
	}

	return nil
}

func findTables(ctx context.Context, reader IndexReader, prefix string) ([]string, error) {
	tables, err := reader.IndexTableNames(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: sort by parsed day index
	out := []string{}
	for _, t := range tables {
		if strings.HasPrefix(t, prefix) {
			out = append(out, t)
		}
	}
	return out, nil
}

func scanSingleTable(ctx context.Context, indexReader IndexReader, tableName string, outDir string, concurrency int, openFiles prometheus.Gauge, series prometheus.Counter) error {
	files := newOpenFiles(1024*1024, openFiles)

	var ps []IndexEntryProcessor

	for i := 0; i < concurrency; i++ {
		ps = append(ps, newProcessor(outDir, files, series))
	}

	err := indexReader.ReadIndexEntries(ctx, tableName, ps)
	if err != nil {
		return err
	}

	files.closeAllFiles(func() interface{} {
		return PlanFooter{Complete: true}
	})

	return nil
}
