package topic

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateName(t *testing.T) {
	valid := []string{"sala-1/lances", "sala-1/placar", "a", "x_y", strings.Repeat("a", MaxNameLength)}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, esperado nil", name, err)
		}
	}
	invalid := []string{"", "Sala-1", "sala 1", "sala.1", "reação", strings.Repeat("a", MaxNameLength+1)}
	for _, name := range invalid {
		if err := ValidateName(name); !errors.Is(err, ErrInvalidName) {
			t.Errorf("ValidateName(%q) = %v, esperado ErrInvalidName", name, err)
		}
	}
}

func TestDefaultPolicy(t *testing.T) {
	cases := []struct {
		kind Kind
		want Policy
	}{
		{KindEphemeral, Policy{MaxTTL: 500 * time.Millisecond, QueueCapacity: 256, Overflow: DropOldest}},
		{KindState, Policy{MaxTTL: 2 * time.Second, QueueCapacity: 1, Overflow: ReplacePending}},
		{Kind(0), Policy{}},
	}
	for _, c := range cases {
		if got := DefaultPolicy(c.kind); got != c.want {
			t.Errorf("DefaultPolicy(%v) = %+v, esperado %+v", c.kind, got, c.want)
		}
	}
}

func TestSpecValidate(t *testing.T) {
	cases := []struct {
		name string
		spec Spec
		want error
	}{
		{"efêmero padrão", Spec{Name: "sala-1/reacoes", Kind: KindEphemeral}, nil},
		{"estado com validade própria", Spec{Name: "sala-1/placar", Kind: KindState, MaxTTL: 5 * time.Second}, nil},
		{"nome inválido", Spec{Name: "Sala", Kind: KindEphemeral}, ErrInvalidName},
		{"tipo ausente", Spec{Name: "sala"}, ErrInvalidKind},
		{"tipo desconhecido", Spec{Name: "sala", Kind: 9}, ErrInvalidKind},
		{"validade negativa", Spec{Name: "sala", Kind: KindState, MaxTTL: -time.Second}, ErrInvalidTTL},
		{"validade abaixo de 1 ms", Spec{Name: "sala", Kind: KindState, MaxTTL: time.Microsecond}, ErrInvalidTTL},
		{"validade acima de uint32 ms", Spec{Name: "sala", Kind: KindState, MaxTTL: maxTTL + time.Millisecond}, ErrInvalidTTL},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.spec.Validate()
			if c.want == nil && err != nil || c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("Validate() = %v, esperado %v", err, c.want)
			}
		})
	}
}

func TestSpecPolicyOverridesOnlyTTL(t *testing.T) {
	p := Spec{Name: "sala", Kind: KindEphemeral, MaxTTL: time.Second}.Policy()
	want := Policy{MaxTTL: time.Second, QueueCapacity: EphemeralQueueCapacity, Overflow: DropOldest}
	if p != want {
		t.Fatalf("Policy() = %+v, esperado %+v", p, want)
	}
}

func TestAcceptsTTL(t *testing.T) {
	info := Info{Kind: KindEphemeral, Policy: DefaultPolicy(KindEphemeral)}
	cases := map[time.Duration]bool{
		0:                      false,
		-time.Millisecond:      false,
		time.Millisecond:       true,
		500 * time.Millisecond: true,
		501 * time.Millisecond: false,
	}
	for ttl, want := range cases {
		if got := info.AcceptsTTL(ttl); got != want {
			t.Errorf("AcceptsTTL(%v) = %v, esperado %v", ttl, got, want)
		}
	}
}
