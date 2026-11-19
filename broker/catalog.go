package broker

import (
	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/peer"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// changeTopic aplica CreateTopic ou DeleteTopic conforme o papel do broker:
// sem par, localmente; no administrativo, replicando antes de aplicar; no
// outro, repassando ao administrativo.
func (b *Broker) changeTopic(req peer.Request) peer.Result {
	switch {
	case b.link == nil:
		return b.applyLocally(req)
	case b.link.Admin():
		return b.replicateAndApply(req)
	}
	res, err := b.link.Forward(req)
	if err != nil {
		return peer.Result{Status: pb.Status_STATUS_UNAVAILABLE, Detail: "broker administrativo indisponível"}
	}
	return res
}

func (b *Broker) applyLocally(req peer.Request) peer.Result {
	var info topic.Info
	var err error
	var removed []contract.Subscription

	b.topicMu.Lock()
	switch req.Kind {
	case peer.OpCreate:
		info, err = b.catalog.Create(req.Spec)
	case peer.OpDelete:
		info, err = b.catalog.Delete(req.Name)
		if err == nil {
			removed = b.subs.RemoveRef(info.Ref)
		}
	}
	b.topicMu.Unlock()

	b.logChange(req.Kind, info, err, "local")
	b.notifyEnded(removed, "exclusão")
	return result(info, err)
}

// replicateAndApply roda no broker administrativo. adminMu serializa as
// alterações, de modo que a versão planejada é a seguinte à atual e o
// snapshot enviado numa reconexão nunca fica entre a confirmação do par e a
// aplicação local.
func (b *Broker) replicateAndApply(req peer.Request) peer.Result {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()

	var info topic.Info
	var err error
	switch req.Kind {
	case peer.OpCreate:
		info, err = b.catalog.PlanCreate(req.Spec)
	case peer.OpDelete:
		info, err = b.catalog.PlanDelete(req.Name)
	}
	if err != nil {
		return result(info, err)
	}

	u := peer.Update{OperationID: req.OperationID, Version: b.catalog.Version() + 1, Kind: req.Kind, Topic: info}
	if err := b.link.Replicate(u); err != nil {
		b.log.Warn("alteração do catálogo não aplicada: par indisponível ou sem confirmação", "operacao", opName(req.Kind),
			"topico", info.Name, "erro", err)
		return peer.Result{Status: pb.Status_STATUS_UNAVAILABLE, Detail: "par indisponível ou sem confirmação"}
	}

	var removed []contract.Subscription
	b.topicMu.Lock()
	switch req.Kind {
	case peer.OpCreate:
		err = b.catalog.Put(info, u.Version)
	case peer.OpDelete:
		_, err = b.catalog.Remove(info.Ref, u.Version)
		removed = b.subs.RemoveRef(info.Ref)
	}
	b.topicMu.Unlock()

	b.logChange(req.Kind, info, err, "replicada ao par")
	b.notifyEnded(removed, "exclusão")
	return result(info, err)
}

func opName(k peer.OpKind) string {
	if k == peer.OpCreate {
		return "criação"
	}
	return "exclusão"
}

// logChange registra uma alteração do catálogo aplicada localmente.
func (b *Broker) logChange(k peer.OpKind, info topic.Info, err error, origin string) {
	if err != nil {
		return
	}
	msg := "tópico criado"
	if k == peer.OpDelete {
		msg = "tópico apagado"
	}
	b.log.Info(msg, "topico", info.Name, "id", uint32(info.ID), "geracao", uint32(info.Generation),
		"versao", b.catalog.Version(), "origem", origin)
}

func result(info topic.Info, err error) peer.Result {
	if err != nil {
		return peer.Result{Status: topicErrorStatus(err), Detail: err.Error()}
	}
	return peer.Result{Topic: info, Status: pb.Status_STATUS_OK}
}

// notifyEnded avisa as sessões cujas inscrições foram encerradas pela
// exclusão do tópico.
func (b *Broker) notifyEnded(removed []contract.Subscription, why string) {
	if len(removed) > 0 {
		b.log.Info("inscrições encerradas", "motivo", why, "quantidade", len(removed))
	}
	for _, sub := range removed {
		b.push(sub.Session, &pb.Envelope{Body: &pb.Envelope_SubscriptionEnded{SubscriptionEnded: &pb.SubscriptionEnded{
			Topic:  protoconv.TopicInfoToProto(sub.Topic),
			Reason: pb.Status_STATUS_TOPIC_NOT_FOUND,
		}}})
	}
}

// withdrawInterest retira o interesse dos tópicos que ficaram sem
// assinantes locais após a queda de uma conexão.
func (b *Broker) withdrawInterest(refs []topic.Ref) {
	b.interestMu.Lock()
	defer b.interestMu.Unlock()
	for _, ref := range refs {
		if !b.subs.HasLocal(ref) && b.link.Link().Alive {
			b.link.Announce(ref, false)
		}
	}
}

// peerStore expõe ao enlace o estado do broker sem ampliar a API pública.
type peerStore struct{ b *Broker }

var _ peer.Store = peerStore{}

func (s peerStore) CatalogSnapshot() topic.Snapshot {
	s.b.adminMu.Lock()
	defer s.b.adminMu.Unlock()
	return s.b.catalog.Snapshot()
}

func (s peerStore) CatalogVersion() uint64 { return s.b.catalog.Version() }

func (s peerStore) ApplyUpdate(u peer.Update) error {
	b := s.b
	var err error
	var removed []contract.Subscription
	b.topicMu.Lock()
	switch u.Kind {
	case peer.OpCreate:
		err = b.catalog.Put(u.Topic, u.Version)
	case peer.OpDelete:
		if _, err = b.catalog.Remove(u.Topic.Ref, u.Version); err == nil {
			removed = b.subs.RemoveRef(u.Topic.Ref)
		}
	}
	b.topicMu.Unlock()
	b.logChange(u.Kind, u.Topic, err, "recebida do par")
	b.notifyEnded(removed, "exclusão")
	return err
}

// ApplySnapshot substitui o catálogo e encerra as inscrições dos tópicos que
// não existem mais com a mesma geração.
func (s peerStore) ApplySnapshot(snap topic.Snapshot) error {
	b := s.b
	var removed []contract.Subscription
	b.topicMu.Lock()
	superseded, err := b.catalog.Restore(snap)
	for _, info := range superseded {
		removed = append(removed, b.subs.RemoveRef(info.Ref)...)
	}
	b.topicMu.Unlock()
	if len(superseded) > 0 {
		b.log.Info("tópicos removidos pelo snapshot do par", "quantidade", len(superseded))
	}
	b.notifyEnded(removed, "snapshot do par")
	return err
}

func (s peerStore) WithLocalInterest(fn func([]topic.Ref)) {
	s.b.interestMu.Lock()
	defer s.b.interestMu.Unlock()
	fn(s.b.subs.Refs())
}

func (s peerStore) Execute(req peer.Request) peer.Result {
	if req.Kind == peer.OpCreate {
		if err := req.Spec.Validate(); err != nil {
			return result(topic.Info{}, err)
		}
	}
	return s.b.replicateAndApply(req)
}
