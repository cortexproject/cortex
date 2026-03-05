package alertmanager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/template"
	commoncfg "github.com/prometheus/common/config"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertspb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/users"
)

const (
	errMarshallingYAML       = "error marshalling YAML Alertmanager config"
	errValidatingConfig      = "error validating Alertmanager config"
	errReadingConfiguration  = "unable to read the Alertmanager config"
	errStoringConfiguration  = "unable to store the Alertmanager config"
	errDeletingConfiguration = "unable to delete the Alertmanager config"
	errNoOrgID               = "unable to determine the OrgID"
	errListAllUser           = "unable to list the Alertmanager users"
	errConfigurationTooBig   = "Alertmanager configuration is too big, limit: %d bytes"
	errTooManyTemplates      = "too many templates in the configuration: %d (limit: %d)"
	errTemplateTooBig        = "template %s is too big: %d bytes (limit: %d bytes)"

	fetchConcurrency = 16
)

var (
	// errBearerTokenAndCredentialsFileNotAllowed covers both bearer_token_file and credentials_file
	// because prometheus/common normalizes bearer_token_file to authorization.credentials_file
	// during YAML unmarshaling (see HTTPClientConfig.Validate in prometheus/common/config).
	errBearerTokenAndCredentialsFileNotAllowed  = errors.New("setting bearer_token_file and credentials_file is not allowed")
	errPasswordFileNotAllowed                   = errors.New("setting password_file is not allowed")
	errUsernameFileNotAllowed                   = errors.New("setting username_file is not allowed")
	errOAuth2CertificateKeyFileNotAllowed       = errors.New("setting OAuth2 client_certificate_key_file is not allowed")
	errOAuth2SecretFileNotAllowed               = errors.New("setting OAuth2 client_secret_file is not allowed")
	errTLSFileNotAllowed                        = errors.New("setting TLS ca_file, cert_file and key_file is not allowed")
	errSlackAPIURLFileNotAllowed                = errors.New("setting Slack api_url_file and global slack_api_url_file is not allowed")
	errSlackAppTokenFileNotAllowed              = errors.New("setting Slack slack_app_token_file and global slack_app_token_file is not allowed")
	errVictorOpsAPIKeyFileNotAllowed            = errors.New("setting VictorOps api_key_file and global victorops_api_key_file is not allowed")
	errOpsGenieAPIKeyFileNotAllowed             = errors.New("setting OpsGenie api_key_file and global opsgenie_api_key_file is not allowed")
	errPagerDutyRoutingKeyFileNotAllowed        = errors.New("setting PagerDuty routing_key_file is not allowed")
	errPagerDutyServiceKeyFileNotAllowed        = errors.New("setting PagerDuty service_key_file is not allowed")
	errWebhookURLFileNotAllowed                 = errors.New("setting Webhook url_file is not allowed")
	errPushOverUserKeyFileNotAllowed            = errors.New("setting PushOver user_key_file is not allowed")
	errPushOverTokenFileNotAllowed              = errors.New("setting PushOver token_file is not allowed")
	errTelegramBotTokenFileNotAllowed           = errors.New("setting Telegram bot_token_file and global telegram_bot_token_file are not allowed")
	errTelegramChatIdFileNotAllowed             = errors.New("setting Telegram chat_id_file is not allowed")
	errMSTeamsWebhookUrlFileNotAllowed          = errors.New("setting MSTeams webhook_url_file is not allowed")
	errMSTeamsV2WebhookUrlFileNotAllowed        = errors.New("setting MSTeamsV2 webhook_url_file is not allowed")
	errRocketChatTokenIdFileNotAllowed          = errors.New("setting RocketChat token_id_file and global rocketchat_token_id_file is not allowed")
	errRocketChatTokenFileNotAllowed            = errors.New("setting RocketChat token_file and global rocketchat_token_file is not allowed")
	errDiscordWebhookUrlFileNotAllowed          = errors.New("setting Discord webhook_url_file is not allowed")
	errEmailAuthPasswordFileNotAllowed          = errors.New("setting Email auth_password_file and global smtp_auth_password_file is not allowed")
	errEmailAuthSecretFileNotAllowed            = errors.New("setting Email auth_secret_file and global smtp_auth_secret_file is not allowed")
	errIncidentIOURLFileNotAllowed              = errors.New("setting IncidentIO url_file is not allowed")
	errIncidentIOAlertSourceTokenFileNotAllowed = errors.New("setting IncidentIO alert_source_token_file is not allowed")
	errMatterMostWebhookUrlFileNotAllowed       = errors.New("setting Mattermost webhook_url_file is not allowed")
	errWeChatAPISecretFileNotAllowed            = errors.New("setting Wechat api_secret_file and global wechat_api_secret_file is not allowed")
)

// UserConfig is used to communicate a users alertmanager configs
type UserConfig struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

func (am *MultitenantAlertmanager) GetUserConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)

	userID, err := users.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	cfg, err := am.store.GetAlertConfig(r.Context(), userID)
	if err != nil {
		switch err {
		case alertspb.ErrNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case alertspb.ErrAccessDenied:
			http.Error(w, err.Error(), http.StatusForbidden)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	d, err := yaml.Marshal(&UserConfig{
		TemplateFiles:      alertspb.ParseTemplates(cfg),
		AlertmanagerConfig: cfg.RawConfig,
	})

	if err != nil {
		level.Error(logger).Log("msg", errMarshallingYAML, "err", err, "user", userID)
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingYAML, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *MultitenantAlertmanager) SetUserConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := users.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	var input io.Reader
	maxConfigSize := am.limits.AlertmanagerMaxConfigSize(userID)
	if maxConfigSize > 0 {
		// LimitReader will return EOF after reading specified number of bytes. To check if
		// we have read too many bytes, allow one extra byte.
		input = io.LimitReader(r.Body, int64(maxConfigSize)+1)
	} else {
		input = r.Body
	}

	payload, err := io.ReadAll(input)
	if err != nil {
		level.Error(logger).Log("msg", errReadingConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errReadingConfiguration, err.Error()), http.StatusBadRequest)
		return
	}

	if maxConfigSize > 0 && len(payload) > maxConfigSize {
		msg := fmt.Sprintf(errConfigurationTooBig, maxConfigSize)
		level.Warn(logger).Log("msg", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	cfg := &UserConfig{}
	err = yaml.Unmarshal(payload, cfg)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingYAML, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errMarshallingYAML, err.Error()), http.StatusBadRequest)
		return
	}

	cfgDesc := alertspb.ToProto(cfg.AlertmanagerConfig, cfg.TemplateFiles, userID)
	if err := validateUserConfig(logger, cfgDesc, am.limits, userID); err != nil {
		level.Warn(logger).Log("msg", errValidatingConfig, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errValidatingConfig, err.Error()), http.StatusBadRequest)
		return
	}

	err = am.store.SetAlertConfig(r.Context(), cfgDesc)
	if err != nil {
		level.Error(logger).Log("msg", errStoringConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errStoringConfiguration, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// DeleteUserConfig is exposed via user-visible API (if enabled, uses DELETE method), but also as an internal endpoint using POST method.
// Note that if no config exists for a user, StatusOK is returned.
func (am *MultitenantAlertmanager) DeleteUserConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := users.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errNoOrgID, err.Error()), http.StatusUnauthorized)
		return
	}

	err = am.store.DeleteAlertConfig(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("msg", errDeletingConfiguration, "err", err.Error())
		http.Error(w, fmt.Sprintf("%s: %s", errDeletingConfiguration, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Partially copied from: https://github.com/prometheus/alertmanager/blob/8e861c646bf67599a1704fc843c6a94d519ce312/cli/check_config.go#L65-L96
func validateUserConfig(logger log.Logger, cfg alertspb.AlertConfigDesc, limits Limits, user string) error {
	// We don't have a valid use case for empty configurations. If a tenant does not have a
	// configuration set and issue a request to the Alertmanager, we'll a) upload an empty
	// config and b) immediately start an Alertmanager instance for them if a fallback
	// configuration is provisioned.
	if cfg.RawConfig == "" {
		return fmt.Errorf("configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint")
	}

	amCfg, err := config.Load(cfg.RawConfig)
	if err != nil {
		return err
	}

	// Validate the config recursively scanning it.
	if err := validateAlertmanagerConfig(amCfg); err != nil {
		return err
	}

	// Validate templates referenced in the alertmanager config.
	for _, name := range amCfg.Templates {
		if err := validateTemplateFilename(name); err != nil {
			return err
		}
	}

	// Check template limits.
	if l := limits.AlertmanagerMaxTemplatesCount(user); l > 0 && len(cfg.Templates) > l {
		return fmt.Errorf(errTooManyTemplates, len(cfg.Templates), l)
	}

	if maxSize := limits.AlertmanagerMaxTemplateSize(user); maxSize > 0 {
		for _, tmpl := range cfg.Templates {
			if size := len(tmpl.GetBody()); size > maxSize {
				return fmt.Errorf(errTemplateTooBig, tmpl.GetFilename(), size, maxSize)
			}
		}
	}

	// Validate template files.
	for _, tmpl := range cfg.Templates {
		if err := validateTemplateFilename(tmpl.Filename); err != nil {
			return err
		}
	}

	// Create templates on disk in a temporary directory.
	// Note: This means the validation will succeed if we can write to tmp but
	// not to configured data dir, and on the flipside, it'll fail if we can't write
	// to tmpDir. Ignoring both cases for now as they're ultra rare but will revisit if
	// we see this in the wild.
	userTempDir, err := os.MkdirTemp("", "validate-config-"+cfg.User)
	if err != nil {
		return err
	}
	defer os.RemoveAll(userTempDir)

	for _, tmpl := range cfg.Templates {
		templateFilepath, err := safeTemplateFilepath(userTempDir, tmpl.Filename)
		if err != nil {
			level.Error(logger).Log("msg", "unable to create template file path", "err", err, "user", cfg.User)
			return err
		}

		if _, err = storeTemplateFile(templateFilepath, tmpl.Body); err != nil {
			level.Error(logger).Log("msg", "unable to store template file", "err", err, "user", cfg.User)
			return fmt.Errorf("unable to store template file '%s'", tmpl.Filename)
		}
	}

	templateFiles := make([]string, len(amCfg.Templates))
	for i, t := range amCfg.Templates {
		templateFiles[i] = filepath.Join(userTempDir, t)
	}

	_, err = template.FromGlobs(templateFiles)
	if err != nil {
		return err
	}

	// Note: Not validating the MultitenantAlertmanager.transformConfig function as that
	// that function shouldn't break configuration. Only way it can fail is if the base
	// autoWebhookURL itself is broken. In that case, I would argue, we should accept the config
	// not reject it.

	return nil
}

func (am *MultitenantAlertmanager) ListAllConfigs(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userIDs, err := am.store.ListAllUsers(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", "failed to list users of alertmanager", "err", err)
		http.Error(w, fmt.Sprintf("%s: %s", errListAllUser, err.Error()), http.StatusInternalServerError)
		return
	}

	done := make(chan struct{})
	iter := make(chan any)

	go func() {
		util.StreamWriteYAMLResponse(w, iter, logger)
		close(done)
	}()

	err = concurrency.ForEachUser(r.Context(), userIDs, fetchConcurrency, func(ctx context.Context, userID string) error {
		cfg, err := am.store.GetAlertConfig(ctx, userID)
		if errors.Is(err, alertspb.ErrNotFound) || errors.Is(err, alertspb.ErrAccessDenied) {
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "failed to fetch alertmanager config for user %s", userID)
		}
		data := map[string]*UserConfig{
			userID: {
				TemplateFiles:      alertspb.ParseTemplates(cfg),
				AlertmanagerConfig: cfg.RawConfig,
			},
		}

		select {
		case iter <- data:
		case <-done: // stop early, if sending response has already finished
		}

		return nil
	})
	if err != nil {
		level.Error(logger).Log("msg", "failed to list all alertmanager configs", "err", err)
	}
	close(iter)
	<-done
}

// noopValidator is a sentinel to mark known non-receiver config types that should be ignored.
var noopValidator = func(any) error { return nil }

// configValidators maps config types to their validation functions.
// - Actual validator: receiver config with file-based fields to validate
// - nil: receiver config with no file-based fields to validate
// - noopValidator: non-receiver config type to ignore
var configValidators = map[reflect.Type]func(any) error{
	reflect.TypeFor[config.GlobalConfig]():        func(v any) error { return validateGlobalConfig(v.(config.GlobalConfig)) },
	reflect.TypeFor[commoncfg.HTTPClientConfig](): func(v any) error { return validateReceiverHTTPConfig(v.(commoncfg.HTTPClientConfig)) },
	reflect.TypeFor[commoncfg.TLSConfig]():        func(v any) error { return validateReceiverTLSConfig(v.(commoncfg.TLSConfig)) },
	reflect.TypeFor[config.OpsGenieConfig]():      func(v any) error { return validateOpsGenieConfig(v.(config.OpsGenieConfig)) },
	reflect.TypeFor[config.SlackConfig]():         func(v any) error { return validateSlackConfig(v.(config.SlackConfig)) },
	reflect.TypeFor[config.VictorOpsConfig]():     func(v any) error { return validateVictorOpsConfig(v.(config.VictorOpsConfig)) },
	reflect.TypeFor[config.PagerdutyConfig]():     func(v any) error { return validatePagerdutyConfig(v.(config.PagerdutyConfig)) },
	reflect.TypeFor[config.WebhookConfig]():       func(v any) error { return validateWebhookConfig(v.(config.WebhookConfig)) },
	reflect.TypeFor[config.PushoverConfig]():      func(v any) error { return validatePushOverConfig(v.(config.PushoverConfig)) },
	reflect.TypeFor[config.TelegramConfig]():      func(v any) error { return validateTelegramConfig(v.(config.TelegramConfig)) },
	reflect.TypeFor[config.MSTeamsConfig]():       func(v any) error { return validateMSTeamsConfig(v.(config.MSTeamsConfig)) },
	reflect.TypeFor[config.MSTeamsV2Config]():     func(v any) error { return validateMSTeamsV2Config(v.(config.MSTeamsV2Config)) },
	reflect.TypeFor[config.RocketchatConfig]():    func(v any) error { return validateRocketChatConfig(v.(config.RocketchatConfig)) },
	reflect.TypeFor[config.DiscordConfig]():       func(v any) error { return validateDiscordConfig(v.(config.DiscordConfig)) },
	reflect.TypeFor[config.EmailConfig]():         func(v any) error { return validateEmailConfig(v.(config.EmailConfig)) },
	reflect.TypeFor[config.IncidentioConfig]():    func(v any) error { return validateIncidentIOConfig(v.(config.IncidentioConfig)) },
	reflect.TypeFor[config.MattermostConfig]():    func(v any) error { return validateMattermostConfig(v.(config.MattermostConfig)) },
	reflect.TypeFor[config.WechatConfig]():        func(v any) error { return validateWeChatConfig(v.(config.WechatConfig)) },
	reflect.TypeFor[config.WebexConfig]():         nil, // No file-based fields to validate
	reflect.TypeFor[config.SNSConfig]():           nil, // No file-based fields to validate
	reflect.TypeFor[config.JiraConfig]():          nil, // No file-based fields to validate
	// Non-receiver config types (ignored during validation)
	reflect.TypeFor[config.Config]():          noopValidator,
	reflect.TypeFor[config.NotifierConfig]():  noopValidator,
	reflect.TypeFor[config.TracingConfig]():   noopValidator,
	reflect.TypeFor[config.ThreadingConfig](): noopValidator,
	reflect.TypeFor[config.JiraFieldConfig](): noopValidator,
}

// validateAlertmanagerConfig recursively scans the input config looking for data types for which
// we have a specific validation and, whenever encountered, it runs their validation. Returns the
// first error or nil if validation succeeds.
func validateAlertmanagerConfig(cfg any) error {
	v := reflect.ValueOf(cfg)
	t := v.Type()

	// Skip invalid, the zero value or a nil pointer (checked by zero value).
	if !v.IsValid() || v.IsZero() {
		return nil
	}

	// If the input config is a pointer then we need to get its value.
	// At this point the pointer value can't be nil.
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	// Check if the input config is a data type for which we have a specific validation.
	if validator, ok := configValidators[t]; ok {
		if validator != nil {
			if err := validator(v.Interface()); err != nil {
				return err
			}
		}
	} else if strings.Contains(t.PkgPath(), "alertmanager/config") &&
		strings.HasSuffix(t.Name(), "Config") {
		// Unhandled config type - fail to ensure we don't miss new types.
		return fmt.Errorf("unsupported receiver config type: %s", t.Name())
	}

	// If the input config is a struct, recursively iterate on all fields.
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldValue := v.FieldByIndex(field.Index)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := validateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		for i := 0; i < v.Len(); i++ {
			fieldValue := v.Index(i)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := validateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	if t.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			fieldValue := v.MapIndex(key)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := validateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// validateReceiverHTTPConfig validates the HTTP config and returns an error if it contains
// settings not allowed by Cortex.
func validateReceiverHTTPConfig(cfg commoncfg.HTTPClientConfig) error {
	if cfg.BasicAuth != nil && cfg.BasicAuth.PasswordFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.BasicAuth != nil && cfg.BasicAuth.UsernameFile != "" {
		return errUsernameFileNotAllowed
	}
	if cfg.Authorization != nil && cfg.Authorization.CredentialsFile != "" {
		return errBearerTokenAndCredentialsFileNotAllowed
	}
	if cfg.BearerTokenFile != "" {
		return errBearerTokenAndCredentialsFileNotAllowed
	}
	if cfg.OAuth2 != nil && cfg.OAuth2.ClientCertificateKeyFile != "" {
		return errOAuth2CertificateKeyFileNotAllowed
	}
	if cfg.OAuth2 != nil && cfg.OAuth2.ClientSecretFile != "" {
		return errOAuth2SecretFileNotAllowed
	}
	return validateReceiverTLSConfig(cfg.TLSConfig)
}

// validateReceiverTLSConfig validates the TLS config and returns an error if it contains
// settings not allowed by Cortex.
func validateReceiverTLSConfig(cfg commoncfg.TLSConfig) error {
	if cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" {
		return errTLSFileNotAllowed
	}
	return nil
}

// validateGlobalConfig validates the Global config and returns an error if it contains
// settings not allowed by Cortex.
func validateGlobalConfig(cfg config.GlobalConfig) error {
	if cfg.OpsGenieAPIKeyFile != "" {
		return errOpsGenieAPIKeyFileNotAllowed
	}
	if cfg.RocketchatTokenFile != "" {
		return errRocketChatTokenFileNotAllowed
	}
	if cfg.RocketchatTokenIDFile != "" {
		return errRocketChatTokenIdFileNotAllowed
	}
	if cfg.SlackAPIURLFile != "" {
		return errSlackAPIURLFileNotAllowed
	}
	if cfg.SlackAppTokenFile != "" {
		return errSlackAppTokenFileNotAllowed
	}
	if cfg.SMTPAuthPasswordFile != "" {
		return errEmailAuthPasswordFileNotAllowed
	}
	if cfg.SMTPAuthSecretFile != "" {
		return errEmailAuthSecretFileNotAllowed
	}
	if cfg.TelegramBotTokenFile != "" {
		return errTelegramBotTokenFileNotAllowed
	}
	if cfg.VictorOpsAPIKeyFile != "" {
		return errVictorOpsAPIKeyFileNotAllowed
	}
	if cfg.WeChatAPISecretFile != "" {
		return errWeChatAPISecretFileNotAllowed
	}
	return nil
}

// validateOpsGenieConfig validates the OpsGenie config and returns an error if it contains
// settings not allowed by Cortex.
func validateOpsGenieConfig(cfg config.OpsGenieConfig) error {
	if cfg.APIKeyFile != "" {
		return errOpsGenieAPIKeyFileNotAllowed
	}
	return nil
}

// validateSlackConfig validates the Slack config and returns an error if it contains
// settings not allowed by Cortex.
func validateSlackConfig(cfg config.SlackConfig) error {
	if cfg.APIURLFile != "" {
		return errSlackAPIURLFileNotAllowed
	}
	if cfg.AppTokenFile != "" {
		return errSlackAppTokenFileNotAllowed
	}
	return nil
}

// validateVictorOpsConfig validates the VictorOps config and returns an error if it contains
// settings not allowed by Cortex.
func validateVictorOpsConfig(cfg config.VictorOpsConfig) error {
	if cfg.APIKeyFile != "" {
		return errVictorOpsAPIKeyFileNotAllowed
	}
	return nil
}

// validatePagerdutyConfig validates the pager duty config and returns an error if it contains
// settings not allowed by Cortex.
func validatePagerdutyConfig(cfg config.PagerdutyConfig) error {
	if cfg.RoutingKeyFile != "" {
		return errPagerDutyRoutingKeyFileNotAllowed
	}

	if cfg.ServiceKeyFile != "" {
		return errPagerDutyServiceKeyFileNotAllowed
	}

	return nil
}

// validateWebhookConfig validates the Webhook config and returns an error if it contains
// settings not allowed by Cortex.
func validateWebhookConfig(cfg config.WebhookConfig) error {
	if cfg.URLFile != "" {
		return errWebhookURLFileNotAllowed
	}
	return nil
}

// validatePushOverConfig validates the Pushover Config and returns an error if it contains
// settings not allowed by Cortex.
func validatePushOverConfig(cfg config.PushoverConfig) error {
	if cfg.UserKeyFile != "" {
		return errPushOverUserKeyFileNotAllowed
	}

	if cfg.TokenFile != "" {
		return errPushOverTokenFileNotAllowed
	}

	return nil
}

// validateTelegramConfig validates the Telegram Config and returns an error if it contains
// settings not allowed by Cortex.
func validateTelegramConfig(cfg config.TelegramConfig) error {
	if cfg.BotTokenFile != "" {
		return errTelegramBotTokenFileNotAllowed
	}
	if cfg.ChatIDFile != "" {
		return errTelegramChatIdFileNotAllowed
	}
	return nil
}

// validateMSTeamsConfig validates the MSTeams Config and returns an error if it contains
// settings not allowed by Cortex.
func validateMSTeamsConfig(cfg config.MSTeamsConfig) error {
	if cfg.WebhookURLFile != "" {
		return errMSTeamsWebhookUrlFileNotAllowed
	}
	return nil
}

// validateMSTeamsV2Config validates the MSTeamsV2 Config and returns an error if it contains
// settings not allowed by Cortex.
func validateMSTeamsV2Config(cfg config.MSTeamsV2Config) error {
	if cfg.WebhookURLFile != "" {
		return errMSTeamsV2WebhookUrlFileNotAllowed
	}
	return nil
}

// validateRocketChatConfig validates the RocketChat Config and returns an error if it contains
// settings not allowed by Cortex.
func validateRocketChatConfig(cfg config.RocketchatConfig) error {
	if cfg.TokenIDFile != "" {
		return errRocketChatTokenIdFileNotAllowed
	}

	if cfg.TokenFile != "" {
		return errRocketChatTokenFileNotAllowed
	}

	return nil
}

// validateDiscordConfig validates the Discord Config and returns an error if it contains
// settings not allowed by Cortex.
func validateDiscordConfig(cfg config.DiscordConfig) error {
	if cfg.WebhookURLFile != "" {
		return errDiscordWebhookUrlFileNotAllowed
	}
	return nil
}

// validateEmailConfig validates the Email Config and returns an error if it contains
// settings not allowed by Cortex.
func validateEmailConfig(cfg config.EmailConfig) error {
	if cfg.AuthPasswordFile != "" {
		return errEmailAuthPasswordFileNotAllowed
	}
	if cfg.AuthSecretFile != "" {
		return errEmailAuthSecretFileNotAllowed
	}
	return nil
}

// validateIncidentIOConfig validates the IncidentIO Config and returns an error if it contains
// settings not allowed by Cortex.
func validateIncidentIOConfig(cfg config.IncidentioConfig) error {
	if cfg.URLFile != "" {
		return errIncidentIOURLFileNotAllowed
	}

	if cfg.AlertSourceTokenFile != "" {
		return errIncidentIOAlertSourceTokenFileNotAllowed
	}

	return nil
}

// validateMatterMostConfig validates the Mattermost Config and returns an error if it contains
// settings not allowed by Cortex.
func validateMattermostConfig(cfg config.MattermostConfig) error {
	if cfg.WebhookURLFile != "" {
		return errMatterMostWebhookUrlFileNotAllowed
	}
	return nil
}

// validateWeChatConfig validates the WeChat Config and returns an error if it contains
// settings not allowed by Cortex.
func validateWeChatConfig(cfg config.WechatConfig) error {
	if cfg.APISecretFile != "" {
		return errWeChatAPISecretFileNotAllowed
	}
	return nil
}
