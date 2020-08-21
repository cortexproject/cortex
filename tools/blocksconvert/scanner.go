package blocksconvert

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util/services"
)

type ScannerConfig struct {
	BigtableProject  string
	BigtableInstance string

	TableName       string
	OutputDirectory string
	Concurrency     int
}

func (cfg *ScannerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BigtableProject, "bigtable.project", "", "The Google Cloud Platform project ID. Required.")
	f.StringVar(&cfg.BigtableInstance, "bigtable.instance", "", "The Google Cloud Bigtable instance ID. Required.")
	f.StringVar(&cfg.TableName, "table", "", "Table to generate plan files from.")
	f.StringVar(&cfg.OutputDirectory, "scanner.local-dir", "", "Local directory used for storing temporary plan files (will be deleted and recreated!).")
	f.IntVar(&cfg.Concurrency, "scanner.concurrency", 16, "Number of concurrent index processors.")
}

type Scanner struct {
	services.Service

	cfg         ScannerConfig
	indexReader IndexReader
	reg         prometheus.Registerer
}

func NewScanner(cfg ScannerConfig, l log.Logger, reg prometheus.Registerer) (*Scanner, error) {
	if cfg.BigtableProject == "" || cfg.BigtableInstance == "" {
		return nil, fmt.Errorf("invalid BigTable configuration")
	}

	if cfg.TableName == "" {
		return nil, fmt.Errorf("no table name provided")
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
		reg:         reg,
		indexReader: NewBigtableIndexReader(cfg.BigtableProject, cfg.BigtableInstance, l, reg),
	}

	s.Service = services.NewBasicService(nil, s.running, nil)
	return s, nil
}

func (s *Scanner) running(ctx context.Context) error {
	files := newOpenFiles(1024*1024, s.reg)

	var ps []IndexEntryProcessor

	for i := 0; i < s.cfg.Concurrency; i++ {
		ps = append(ps, newProcessor(s.cfg.OutputDirectory, files, s.reg))
	}

	err := s.indexReader.ReadIndexEntries(ctx, s.cfg.TableName, ps)
	if err != nil {
		return err
	}

	return nil
}
