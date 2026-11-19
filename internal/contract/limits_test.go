package contract

import "testing"

func TestStubs(t *testing.T) {
	s := EmptyStats{}.Stats()
	if s.Counters == nil || s.Gauges == nil {
		t.Fatal("EmptyStats deve devolver mapas vazios, não nulos")
	}
	if !(AllowAllCommands{}).AllowCommand("qualquer") {
		t.Fatal("AllowAllCommands recusou comando")
	}
}
