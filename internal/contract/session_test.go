package contract

import (
	"errors"
	"net/netip"
	"testing"
)

var (
	addrA = netip.MustParseAddrPort("10.0.0.1:40000")
	addrB = netip.MustParseAddrPort("10.0.0.2:40001")
)

func TestBindAndGet(t *testing.T) {
	st := NewSessionTable()
	s := Session{ID: 7, Identity: "ana", UDPAddr: addrA}
	release, err := st.Bind(s)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := st.Get(7); !ok || got != s {
		t.Fatalf("Get(7) = %+v, %v", got, ok)
	}
	release()
	if _, ok := st.Get(7); ok {
		t.Fatal("sessão continua ligada após release")
	}
	release()
}

func TestBindRejectsZeroSession(t *testing.T) {
	if _, err := NewSessionTable().Bind(Session{Identity: "ana"}); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("esperado ErrInvalidSession, obtido %v", err)
	}
}

func TestBindRejectsOtherIdentity(t *testing.T) {
	st := NewSessionTable()
	if _, err := st.Bind(Session{ID: 7, Identity: "ana", UDPAddr: addrA}); err != nil {
		t.Fatal(err)
	}
	_, err := st.Bind(Session{ID: 7, Identity: "bruno", UDPAddr: addrB})
	if !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("esperado ErrSessionConflict, obtido %v", err)
	}
	if got, _ := st.Get(7); got.Identity != "ana" || got.UDPAddr != addrA {
		t.Fatalf("vínculo original alterado: %+v", got)
	}
}

func TestRebindSameIdentityReplacesAndOldReleaseIsHarmless(t *testing.T) {
	st := NewSessionTable()
	releaseOld, err := st.Bind(Session{ID: 7, Identity: "ana", UDPAddr: addrA})
	if err != nil {
		t.Fatal(err)
	}
	releaseNew, err := st.Bind(Session{ID: 7, Identity: "ana", UDPAddr: addrB})
	if err != nil {
		t.Fatal(err)
	}

	releaseOld()
	got, ok := st.Get(7)
	if !ok || got.UDPAddr != addrB {
		t.Fatalf("release antigo removeu o vínculo novo: %+v, %v", got, ok)
	}
	releaseNew()
	if _, ok := st.Get(7); ok {
		t.Fatal("release novo deveria remover o vínculo")
	}
}

func TestReleasedSessionCanBeTakenByOtherIdentity(t *testing.T) {
	st := NewSessionTable()
	release, _ := st.Bind(Session{ID: 7, Identity: "ana"})
	release()
	if _, err := st.Bind(Session{ID: 7, Identity: "bruno"}); err != nil {
		t.Fatalf("sessão livre deveria aceitar outra identidade: %v", err)
	}
}
