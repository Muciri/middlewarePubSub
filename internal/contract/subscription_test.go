package contract

import (
	"testing"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

func info(id topic.ID, gen topic.Generation) topic.Info {
	return topic.Info{Ref: topic.Ref{ID: id, Generation: gen}, Name: "t", Kind: topic.KindEphemeral, Policy: topic.DefaultPolicy(topic.KindEphemeral)}
}

func sessionIDs(subs []Subscription) []SessionID {
	out := make([]SessionID, len(subs))
	for i, s := range subs {
		out[i] = s.Session
	}
	return out
}

func equalIDs(a, b []SessionID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestAddIsIdempotent(t *testing.T) {
	st := NewSubscriptionTable()
	s := Session{ID: 1, Identity: "ana", UDPAddr: addrA}
	first, created := st.Add(s, info(1, 1))
	if !created {
		t.Fatal("primeira inscrição deveria ser criada")
	}
	again, created := st.Add(s, info(1, 1))
	if created || again.ID != first.ID {
		t.Fatalf("repetição criou nova inscrição: %+v", again)
	}
	if got := st.ForTopic(info(1, 1).Ref); len(got) != 1 {
		t.Fatalf("ForTopic devolveu %d inscrições", len(got))
	}
}

func TestForTopicMatchesGeneration(t *testing.T) {
	st := NewSubscriptionTable()
	st.Add(Session{ID: 1, Identity: "ana"}, info(1, 1))
	st.Add(Session{ID: 2, Identity: "bia"}, info(1, 2))
	st.Add(Session{ID: 3, Identity: "caio"}, info(1, 2))

	if got := sessionIDs(st.ForTopic(topic.Ref{ID: 1, Generation: 2})); !equalIDs(got, []SessionID{2, 3}) {
		t.Fatalf("ForTopic(gen 2) = %v", got)
	}
	if !st.HasLocal(topic.Ref{ID: 1, Generation: 1}) || st.HasLocal(topic.Ref{ID: 1, Generation: 3}) {
		t.Fatal("HasLocal não respeita a geração")
	}
}

func TestForTopicCarriesSessionData(t *testing.T) {
	st := NewSubscriptionTable()
	st.Add(Session{ID: 9, Identity: "ana", UDPAddr: addrA}, info(4, 1))
	sub := st.ForTopic(topic.Ref{ID: 4, Generation: 1})[0]
	if sub.Identity != "ana" || sub.UDPAddr != addrA || sub.Topic.Policy.QueueCapacity != topic.EphemeralQueueCapacity {
		t.Fatalf("inscrição incompleta: %+v", sub)
	}
}

func TestRemove(t *testing.T) {
	st := NewSubscriptionTable()
	ref := info(1, 1).Ref
	st.Add(Session{ID: 1}, info(1, 1))

	if _, ok := st.Remove(2, ref); ok {
		t.Fatal("removeu inscrição inexistente")
	}
	if _, ok := st.Remove(1, ref); !ok {
		t.Fatal("não removeu inscrição existente")
	}
	if st.HasLocal(ref) {
		t.Fatal("tópico sem inscrições ainda aparece")
	}
}

func TestRemoveSession(t *testing.T) {
	st := NewSubscriptionTable()
	st.Add(Session{ID: 1}, info(1, 1))
	st.Add(Session{ID: 1}, info(2, 1))
	st.Add(Session{ID: 2}, info(1, 1))

	if removed := st.RemoveSession(1); len(removed) != 2 {
		t.Fatalf("removidas %d inscrições, esperado 2", len(removed))
	}
	if got := sessionIDs(st.ForTopic(info(1, 1).Ref)); !equalIDs(got, []SessionID{2}) {
		t.Fatalf("restaram %v", got)
	}
	if st.HasLocal(info(2, 1).Ref) {
		t.Fatal("tópico 2 deveria ficar sem inscrições")
	}
}

func TestRemoveTopicDropsAllGenerations(t *testing.T) {
	st := NewSubscriptionTable()
	st.Add(Session{ID: 1}, info(1, 1))
	st.Add(Session{ID: 2}, info(1, 2))
	st.Add(Session{ID: 3}, info(2, 1))

	if removed := st.RemoveTopic(1); len(removed) != 2 {
		t.Fatalf("removidas %d inscrições, esperado 2", len(removed))
	}
	if st.HasLocal(info(1, 1).Ref) || st.HasLocal(info(1, 2).Ref) || !st.HasLocal(info(2, 1).Ref) {
		t.Fatal("RemoveTopic afetou o tópico errado")
	}
}

func TestForTopicReturnsCopy(t *testing.T) {
	st := NewSubscriptionTable()
	st.Add(Session{ID: 1, Identity: "ana"}, info(1, 1))
	got := st.ForTopic(info(1, 1).Ref)
	got[0].Identity = "alterado"
	if st.ForTopic(info(1, 1).Ref)[0].Identity != "ana" {
		t.Fatal("ForTopic expôs o estado interno")
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestDoneClosedOnEveryRemovalPath(t *testing.T) {
	st := NewSubscriptionTable()
	byRemove, _ := st.Add(Session{ID: 1}, info(1, 1))
	bySession, _ := st.Add(Session{ID: 2}, info(1, 1))
	byTopic, _ := st.Add(Session{ID: 3}, info(2, 1))
	kept, _ := st.Add(Session{ID: 4}, info(3, 1))

	if isClosed(byRemove.Done()) {
		t.Fatal("Done fechado antes da remoção")
	}
	st.Remove(1, info(1, 1).Ref)
	st.RemoveSession(2)
	st.RemoveTopic(2)

	for name, sub := range map[string]Subscription{"Remove": byRemove, "RemoveSession": bySession, "RemoveTopic": byTopic} {
		if !isClosed(sub.Done()) {
			t.Errorf("%s não fechou Done", name)
		}
	}
	if isClosed(kept.Done()) {
		t.Fatal("inscrição não removida teve Done fechado")
	}
}

func TestDoneSharedByRepeatedAdd(t *testing.T) {
	st := NewSubscriptionTable()
	first, _ := st.Add(Session{ID: 1}, info(1, 1))
	again, _ := st.Add(Session{ID: 1}, info(1, 1))
	st.Remove(1, info(1, 1).Ref)
	if !isClosed(first.Done()) || !isClosed(again.Done()) {
		t.Fatal("as duas cópias deveriam observar o fechamento")
	}
}
