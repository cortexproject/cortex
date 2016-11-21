package ruler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
)

// Config is the configuration for the recording rules server.
type Config struct {
	DistributorConfig distributor.Config
	ConfigsAPIURL     string
	ExternalURL       string
	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	// XXX: Currently single tenant only (which is awful) as the most
	// expedient way of getting *something* working.
	UserID string
}

// Ruler is a recording rules server.
type Ruler struct {
	cfg         Config
	chunkStore  chunk.Store
	distributor *distributor.Distributor

	configsAPIURL *url.URL
	externalURL   *url.URL
}

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

// New returns a new Ruler.
func New(chunkStore chunk.Store, cfg Config) (*Ruler, error) {
	configsAPIURL, err := url.Parse(cfg.ConfigsAPIURL)
	if err != nil {
		return nil, err
	}
	externalURL, err := url.Parse(cfg.ExternalURL)
	if err != nil {
		return nil, err
	}

	d, err := distributor.New(cfg.DistributorConfig)
	if err != nil {
		return nil, err
	}
	return &Ruler{
		cfg:           cfg,
		chunkStore:    chunkStore,
		distributor:   d,
		configsAPIURL: configsAPIURL,
		externalURL:   externalURL,
	}, nil
}

// GetWorkerFor gets a rules recording worker for the given user.
// It will keep polling until it can construct one.
func (r *Ruler) GetWorkerFor(userID string) Worker {
	delay := time.Duration(r.cfg.EvaluationInterval)
	for {
		worker, err := r.getWorkerFor(userID)
		if err == nil {
			return worker
		}
		log.Warnf("Could not get configuration for %v: %v", userID, err)
		time.Sleep(delay)
	}
}

// getWorkerFor gets a rules recording worker for the given user.
func (r *Ruler) getWorkerFor(userID string) (Worker, error) {
	mgr := r.getManager(userID)
	conf, err := r.getConfig(userID)
	if err != nil {
		return nil, fmt.Errorf("Error fetching config: %v", err)
	}
	err = mgr.ApplyConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Error applying config: %v", err)
	}
	return mgr, nil
}

func (r *Ruler) getManager(userID string) *rules.Manager {
	ctx := user.WithID(context.Background(), userID)
	appender := appenderAdapter{distributor: r.distributor, ctx: ctx}
	queryable := querier.NewQueryable(r.distributor, r.chunkStore)
	engine := promql.NewEngine(queryable, nil)
	return rules.NewManager(&rules.ManagerOptions{
		SampleAppender: appender,
		Notifier:       nil,
		QueryEngine:    engine,
		Context:        ctx,
		ExternalURL:    r.externalURL,
	})
}

func (r *Ruler) getConfig(userID string) (*config.Config, error) {
	// XXX: This is highly specific to Weave Cloud. Would be good to have a
	// more pluggable way of expressing this for Cortex.
	cfg, err := getOrgConfig(r.configsAPIURL, userID)
	if err != nil {
		return nil, err
	}

	dir, err := ioutil.TempDir("", fmt.Sprintf("rules-%v", userID))
	if err != nil {
		return nil, err
	}

	ruleFiles := []string{}
	for filename, rules := range cfg.RulesFiles {
		filepath := path.Join(dir, filename)
		err = ioutil.WriteFile(filepath, []byte(rules), 0644)
		if err != nil {
			// XXX: Clean up already-written files
			return nil, err
		}
		ruleFiles = append(ruleFiles, filepath)
	}

	globalConfig := config.DefaultGlobalConfig
	globalConfig.EvaluationInterval = model.Duration(r.cfg.EvaluationInterval)
	return &config.Config{
		GlobalConfig: globalConfig,
		RuleFiles:    ruleFiles,
	}, nil
}

// appenderAdapter adapts a distributor.Distributor to prometheus.SampleAppender
type appenderAdapter struct {
	distributor *distributor.Distributor
	ctx         context.Context
}

func (a appenderAdapter) Append(sample *model.Sample) error {
	req := util.ToWriteRequest([]*model.Sample{sample})
	_, err := a.distributor.Push(a.ctx, req)
	return err
}

func (a appenderAdapter) NeedsThrottling() bool {
	// XXX: Just a guess. Who knows?
	return false
}

type cortexConfig struct {
	RulesFiles map[string]string `json:"rules_files"`
}

// getOrgConfig gets the organization's cortex config from a configs api server.
func getOrgConfig(configsAPIURL *url.URL, userID string) (*cortexConfig, error) {
	// TODO: Extract configs client logic into go client library (ala users)
	// TODO: Fix configs server so that we not need org ID in the URL to get authenticated org
	url := fmt.Sprintf("%s/api/configs/org/%s/cortex", configsAPIURL.String(), userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Scope-OrgID", userID)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	var config cortexConfig
	if err := json.NewDecoder(res.Body).Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}
