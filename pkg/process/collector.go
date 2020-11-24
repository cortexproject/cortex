package process

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
)

type Options struct {
	Pid            int
	ProcMountPoint string
}

type processCollector struct {
	opts Options

	// Metrics.
	currMaps *prometheus.Desc
	maxMaps  *prometheus.Desc
}

// NewProcessCollector makes a new custom process collector used to collect process metrics the
// default instrumentation doesn't support.
func NewProcessCollector(opts Options) prometheus.Collector {
	// Apply default options.
	if opts.Pid == 0 {
		opts.Pid = os.Getpid()
	}
	if opts.ProcMountPoint == "" {
		opts.ProcMountPoint = procfs.DefaultMountPoint
	}

	c := &processCollector{
		opts: opts,
		currMaps: prometheus.NewDesc(
			"process_memory_map_areas",
			"Number of memory map areas allocated by the process.",
			nil, nil,
		),
		maxMaps: prometheus.NewDesc(
			"process_memory_map_areas_limit",
			"Maximum number of memory map ares the process can allocate.",
			nil, nil,
		),
	}

	return c
}

// Describe returns all descriptions of the collector.
func (c *processCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.currMaps
	ch <- c.maxMaps
}

// Collect returns the current state of all metrics of the collector.
func (c *processCollector) Collect(ch chan<- prometheus.Metric) {
	if value, err := c.getMapsCount(); err == nil {
		ch <- prometheus.MustNewConstMetric(c.currMaps, prometheus.GaugeValue, value)
	}

	if value, err := c.getMapsCountLimit(); err == nil {
		ch <- prometheus.MustNewConstMetric(c.maxMaps, prometheus.GaugeValue, value)
	}
}

// getMapsCount returns the number of memory map ares the process has allocated.
func (c *processCollector) getMapsCount() (float64, error) {
	file, err := os.Open(processMapsPath(c.opts.ProcMountPoint, c.opts.Pid))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	for scan := bufio.NewScanner(file); scan.Scan(); {
		count++
	}

	return float64(count), nil
}

// getMapsCountLimit returns the maximum of memory map ares the process can allocate.
func (c *processCollector) getMapsCountLimit() (float64, error) {
	file, err := os.Open(vmMapsLimitPath(c.opts.ProcMountPoint))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return 0, err
	}

	content = bytes.TrimSpace(content)

	// A base value of zero makes ParseInt infer the correct base using the
	// string's prefix, if any.
	const base = 0
	value, err := strconv.ParseInt(string(content), base, 64)
	if err != nil {
		return 0, err
	}

	return float64(value), nil
}

func processMapsPath(procPath string, pid int) string {
	return filepath.Join(procPath, strconv.Itoa(pid), "maps")
}

func vmMapsLimitPath(procPath string) string {
	return filepath.Join(procPath, "sys", "vm", "max_map_count")
}
