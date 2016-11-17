package ruler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/user"
)

// Config is the configuration for the recording rules server.
type Config struct {
	DistributorConfig distributor.Config
	ConfigsAPIURL     string
	ExternalURL       string
	// XXX: Currently single tenant only (which is awful) as the most
	// expedient way of getting *something* working.
	UserID string
	// XXX: UserID can be inferred from token if we have access to the users
	// server, which we do. Just specifying both so I can get something
	// running asap.
	UserToken string
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

// Execute does the thing.
func (r *Ruler) Execute(userID, userToken string) (Worker, error) {
	mgr := r.getManager(userToken)
	conf, err := r.getConfig(userID)
	if err != nil {
		return nil, err
	}
	err = mgr.ApplyConfig(conf)
	if err != nil {
		return nil, err
	}
	return mgr, nil
}

func (r *Ruler) getManager(userID string) *rules.Manager {
	ctx := user.WithID(context.Background(), userID)
	appender := appenderAdapter{appender: r.distributor, ctx: ctx}
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
	// TODO: Extract configs client logic into go client library (ala users)
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
		err = ioutil.WriteFile(filepath, rules, 0644)
		if err != nil {
			// XXX: Clean up already-written files
			return nil, err
		}
		ruleFiles = append(ruleFiles, filepath)
	}

	return &config.Config{
		// XXX: Need to set recording interval here.
		GlobalConfig: config.DefaultGlobalConfig,
		RuleFiles:    ruleFiles,
	}, nil
}

// appenderAdapter adapts cortex.SampleAppender to prometheus.SampleAppender
type appenderAdapter struct {
	appender cortex.SampleAppender
	ctx      context.Context
}

func (a appenderAdapter) Append(sample *model.Sample) error {
	return a.appender.Append(a.ctx, []*model.Sample{sample})
}

func (a appenderAdapter) NeedsThrottling() bool {
	// XXX: Just a guess.
	return false
}

type cortexConfig struct {
	RulesFiles map[string][]byte `json:"rules_files"`
}

// getOrgConfig gets the organization's cortex config from a configs api server.
func getOrgConfig(configsAPIURL *url.URL, userID string) (*cortexConfig, error) {
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
