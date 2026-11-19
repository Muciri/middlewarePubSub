package contract

// StatsSnapshot é o conteúdo da resposta ao GetStats, montado pelo módulo
// diagnostics.
type StatsSnapshot struct {
	Counters map[string]uint64
	Gauges   map[string]int64
}

type StatsSource interface {
	Stats() StatsSnapshot
}

// EmptyStats não tem contadores.
type EmptyStats struct{}

var _ StatsSource = EmptyStats{}

func (EmptyStats) Stats() StatsSnapshot {
	return StatsSnapshot{Counters: map[string]uint64{}, Gauges: map[string]int64{}}
}

// CommandLimiter aplica a cota de comandos por identidade. Comando recusado
// recebe STATUS_LIMIT_EXCEEDED.
type CommandLimiter interface {
	AllowCommand(identity string) bool
}

// AllowAllCommands não aplica cota.
type AllowAllCommands struct{}

var _ CommandLimiter = AllowAllCommands{}

func (AllowAllCommands) AllowCommand(string) bool { return true }
