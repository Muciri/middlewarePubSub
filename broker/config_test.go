package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg, err := Config{ID: "A", Credentials: map[string]Credential{"ana": {Token: "x"}}}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventPort != 7001 || cfg.HeartbeatInterval != time.Second || cfg.HeartbeatTimeout != 5*time.Second ||
		cfg.WriteTimeout != time.Second || cfg.ResultQueue != 4096 || cfg.Stats == nil || cfg.Limiter == nil {
		t.Fatalf("padrões incorretos: %+v", cfg)
	}
	if cfg.RestrictTopicAdmin {
		t.Fatal("restrição de criação e exclusão deve vir desligada")
	}
}

func TestConfigValidation(t *testing.T) {
	creds := map[string]Credential{"ana": {Token: "x"}}
	cases := map[string]Config{
		"sem ID":                 {Credentials: creds},
		"sem credenciais":        {ID: "A"},
		"prazo negativo":         {ID: "A", Credentials: creds, WriteTimeout: -1},
		"intervalo maior limite": {ID: "A", Credentials: creds, HeartbeatInterval: 6 * time.Second},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := cfg.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("esperado ErrInvalidConfig, obtido %v", err)
			}
		})
	}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credenciais.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadCredentials(t *testing.T) {
	path := writeFile(t, `[
		{"identity": "leiloeiro", "token": "t1", "admin": true},
		{"identity": "espectador", "token": "t2"}
	]`)
	creds, err := LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	if creds["leiloeiro"] != (Credential{Token: "t1", Admin: true}) || creds["espectador"] != (Credential{Token: "t2"}) {
		t.Fatalf("credenciais = %+v", creds)
	}
}

func TestLoadCredentialsRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"json inválido": `{`,
		"sem token":     `[{"identity": "ana"}]`,
		"repetida":      `[{"identity": "ana", "token": "a"}, {"identity": "ana", "token": "b"}]`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadCredentials(writeFile(t, content)); err == nil {
				t.Fatal("esperado erro")
			}
		})
	}
}
