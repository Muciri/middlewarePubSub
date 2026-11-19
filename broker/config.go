// Package broker atende o canal de controle dos clientes (porta 7000/TCP):
// autenticação, tópicos, inscrições, resultados de admissão e diagnóstico.
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
)

const (
	DefaultControlPort       = 7000
	DefaultEventPort         = 7001
	DefaultHeartbeatInterval = time.Second
	DefaultHeartbeatTimeout  = 5 * time.Second
	DefaultWriteTimeout      = time.Second
	DefaultResultQueue       = 4096
)

type Credential struct {
	Token string
	Admin bool
}

type Config struct {
	ID string
	// EventPort é a porta UDP anunciada no HelloReply.
	EventPort   uint16
	Credentials map[string]Credential
	// RestrictTopicAdmin limita CreateTopic e DeleteTopic a identidades
	// administrativas.
	RestrictTopicAdmin bool

	HeartbeatInterval time.Duration
	// HeartbeatTimeout é o silêncio do cliente, incluindo a espera pelo
	// Hello, que encerra a conexão.
	HeartbeatTimeout time.Duration
	// WriteTimeout encerra a conexão de um cliente que deixa de ler o canal
	// de controle.
	WriteTimeout time.Duration
	// ResultQueue limita as mensagens enviadas por iniciativa do broker
	// aguardando escrita em cada conexão.
	ResultQueue int

	Stats   contract.StatsSource
	Limiter contract.CommandLimiter
}

var ErrInvalidConfig = errors.New("broker: configuração inválida")

func (c Config) withDefaults() (Config, error) {
	if c.ID == "" {
		return c, fmt.Errorf("%w: ID vazio", ErrInvalidConfig)
	}
	if len(c.Credentials) == 0 {
		return c, fmt.Errorf("%w: nenhuma credencial", ErrInvalidConfig)
	}
	if c.HeartbeatInterval < 0 || c.HeartbeatTimeout < 0 || c.WriteTimeout < 0 || c.ResultQueue < 0 {
		return c, fmt.Errorf("%w: valores negativos", ErrInvalidConfig)
	}
	if c.EventPort == 0 {
		c.EventPort = DefaultEventPort
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.HeartbeatInterval >= c.HeartbeatTimeout {
		return c, fmt.Errorf("%w: intervalo de heartbeat deve ser menor que o limite de silêncio", ErrInvalidConfig)
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.ResultQueue == 0 {
		c.ResultQueue = DefaultResultQueue
	}
	if c.Stats == nil {
		c.Stats = contract.EmptyStats{}
	}
	if c.Limiter == nil {
		c.Limiter = contract.AllowAllCommands{}
	}
	return c, nil
}

type credentialEntry struct {
	Identity string `json:"identity"`
	Token    string `json:"token"`
	Admin    bool   `json:"admin"`
}

// LoadCredentials lê um arquivo JSON com uma lista de objetos
// {"identity", "token", "admin"}.
func LoadCredentials(path string) (map[string]Credential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []credentialEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("broker: credenciais em %s: %w", path, err)
	}
	creds := make(map[string]Credential, len(entries))
	for i, e := range entries {
		if e.Identity == "" || e.Token == "" {
			return nil, fmt.Errorf("broker: credencial %d sem identidade ou token", i)
		}
		if _, dup := creds[e.Identity]; dup {
			return nil, fmt.Errorf("broker: identidade %q repetida", e.Identity)
		}
		creds[e.Identity] = Credential{Token: e.Token, Admin: e.Admin}
	}
	return creds, nil
}
