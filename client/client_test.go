package client

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
)

func validConfig() Config {
	return Config{
		Brokers:        []string{"127.0.0.1:7000", "127.0.0.2:7000"},
		Identity:       "ana",
		Token:          "segredo",
		PublishTimeout: time.Second,
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := validConfig().withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatTimeout != 5*time.Second || cfg.MaxPending != 4096 {
		t.Fatalf("padrões incorretos: %+v", cfg)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]func(*Config){
		"sem brokers":        func(c *Config) { c.Brokers = nil },
		"broker vazio":       func(c *Config) { c.Brokers = []string{"127.0.0.1:7000", ""} },
		"sem identidade":     func(c *Config) { c.Identity = "" },
		"sem prazo":          func(c *Config) { c.PublishTimeout = 0 },
		"heartbeat negativo": func(c *Config) { c.HeartbeatTimeout = -time.Second },
		"pendentes negativo": func(c *Config) { c.MaxPending = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if _, err := cfg.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("esperado ErrInvalidConfig, obtido %v", err)
			}
		})
	}
}

func TestStatusMatchesProto(t *testing.T) {
	for num, name := range pb.AdmissionStatus_name {
		if num == 0 {
			continue
		}
		got := statusFromProto(pb.AdmissionStatus(num))
		if want := strings.TrimPrefix(name, "ADMISSION_"); got.String() != want {
			t.Errorf("proto %s virou %s", name, got)
		}
	}
}

func TestStatusFromProtoOutOfRange(t *testing.T) {
	for _, v := range []pb.AdmissionStatus{0, -1, 10, 99} {
		if got := statusFromProto(v); got != StatusUnknown {
			t.Errorf("statusFromProto(%d) = %v, esperado UNKNOWN", v, got)
		}
	}
}
