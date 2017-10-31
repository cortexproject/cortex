package alertmanager

import (
	"testing"

	amconfig "github.com/prometheus/alertmanager/config"
)

func Test_transformConfig(t *testing.T) {
	fallbackUrl := "internal://monitor"
	internalURL := "http://some-internal-url"

	am := &MultitenantAlertmanager{
		cfg: &MultitenantAlertmanagerConfig{
			AutoSlackRoot: internalURL,
		},
		fallbackConfig: &amconfig.Config{
			Receivers: []*amconfig.Receiver{
				&amconfig.Receiver{
					Name: "my-receiver",
					SlackConfigs: []*amconfig.SlackConfig{
						&amconfig.SlackConfig{
							APIURL: amconfig.Secret(fallbackUrl),
						},
					},
				},
			},
		},
	}

	var amConfig *amconfig.Config

	cfg, err := am.transformConfig("1", amConfig)

	if err != nil {
		t.Fatal(err)
	}

	expected := internalURL + "/1/monitor"
	actual := string(cfg.Receivers[0].SlackConfigs[0].APIURL)

	if actual != expected {
		t.Fatalf("Expected user config to have a modified URL. Expected %v; Actual: %v", expected, actual)
	}

	fallbackConfigUrl := string(am.fallbackConfig.Receivers[0].SlackConfigs[0].APIURL)
	if string(fallbackConfigUrl) != fallbackUrl {
		t.Fatalf("Expected fallbackConfig to not be modified. Expected: %v, Actual: %v", fallbackUrl, fallbackConfigUrl)
	}
}
