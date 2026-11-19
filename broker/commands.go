package broker

import (
	"errors"
	"fmt"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/peer"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

var errProtocol = errors.New("mensagem fora do protocolo")

// dispatch trata uma mensagem após o Hello. Um erro encerra a conexão:
// violação de protocolo ou falha ao responder.
func (c *clientConn) dispatch(req *pb.Envelope) error {
	if req.GetHeartbeat() != nil {
		return nil
	}
	if !isCommand(req) {
		return errProtocol
	}
	var reply *pb.Envelope
	var after func()
	if c.b.cfg.Limiter.AllowCommand(c.session.Identity) {
		reply, after = c.execute(req)
	} else {
		reply = failure(req, pb.Status_STATUS_LIMIT_EXCEEDED, "cota de comandos excedida")
	}
	reply.RequestId = req.RequestId
	err := c.wc.Send(reply)
	if after != nil {
		after()
	}
	if err != nil {
		return fmt.Errorf("resposta: %w", err)
	}
	return nil
}

func isCommand(req *pb.Envelope) bool {
	switch req.Body.(type) {
	case *pb.Envelope_CreateTopic, *pb.Envelope_DeleteTopic, *pb.Envelope_LookupTopic,
		*pb.Envelope_Subscribe, *pb.Envelope_Unsubscribe, *pb.Envelope_GetStats:
		return true
	}
	return false
}

// execute devolve a resposta e, opcionalmente, uma função a chamar depois
// que ela for escrita.
func (c *clientConn) execute(req *pb.Envelope) (*pb.Envelope, func()) {
	switch body := req.Body.(type) {
	case *pb.Envelope_CreateTopic:
		return c.createTopic(req, body.CreateTopic), nil
	case *pb.Envelope_DeleteTopic:
		return c.deleteTopic(req, body.DeleteTopic), nil
	case *pb.Envelope_LookupTopic:
		return c.lookupTopic(req, body.LookupTopic), nil
	case *pb.Envelope_Subscribe:
		return c.subscribe(req, body.Subscribe)
	case *pb.Envelope_Unsubscribe:
		return c.unsubscribe(req, body.Unsubscribe), nil
	case *pb.Envelope_GetStats:
		return c.getStats(req), nil
	}
	panic("broker: comando sem tratamento")
}

func (c *clientConn) createTopic(req *pb.Envelope, m *pb.CreateTopic) *pb.Envelope {
	if c.b.cfg.RestrictTopicAdmin && !c.session.Admin {
		return failure(req, pb.Status_STATUS_FORBIDDEN, "criação restrita a administradores")
	}
	spec := protoconv.SpecFromProto(m)
	if err := spec.Validate(); err != nil {
		return failure(req, pb.Status_STATUS_INVALID_ARGUMENT, err.Error())
	}
	res := c.b.changeTopic(peer.Request{OperationID: peer.NewOperationID(), Kind: peer.OpCreate, Spec: spec})
	if res.Status != pb.Status_STATUS_OK {
		return failure(req, res.Status, res.Detail)
	}
	return &pb.Envelope{Body: &pb.Envelope_CreateTopicReply{CreateTopicReply: &pb.CreateTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(res.Topic),
	}}}
}

func (c *clientConn) deleteTopic(req *pb.Envelope, m *pb.DeleteTopic) *pb.Envelope {
	if c.b.cfg.RestrictTopicAdmin && !c.session.Admin {
		return failure(req, pb.Status_STATUS_FORBIDDEN, "exclusão restrita a administradores")
	}
	res := c.b.changeTopic(peer.Request{OperationID: peer.NewOperationID(), Kind: peer.OpDelete, Name: m.GetName()})
	if res.Status != pb.Status_STATUS_OK {
		return failure(req, res.Status, res.Detail)
	}
	return &pb.Envelope{Body: &pb.Envelope_DeleteTopicReply{DeleteTopicReply: &pb.DeleteTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(res.Topic),
	}}}
}

func (c *clientConn) lookupTopic(req *pb.Envelope, m *pb.LookupTopic) *pb.Envelope {
	if !c.b.catalogReady() {
		return failure(req, pb.Status_STATUS_UNAVAILABLE, "catálogo ainda não sincronizado com o par")
	}
	info, ok := c.b.catalog.Lookup(m.GetName())
	if !ok {
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico não encontrado")
	}
	return &pb.Envelope{Body: &pb.Envelope_LookupTopicReply{LookupTopicReply: &pb.LookupTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
}

// subscribe registra a inscrição e, com par configurado, anuncia o
// interesse quando o tópico passa a ter assinantes locais, confirmando ao
// cliente só depois do ACK. A resposta é escrita com a leitura do catálogo
// ainda travada, para que um aviso de exclusão do mesmo tópico não chegue
// ao cliente antes da confirmação.
//
// A restauração após reconexão é aceita com o enlace fora: o interesse
// segue no snapshot de interesse quando o enlace voltar.
func (c *clientConn) subscribe(req *pb.Envelope, m *pb.Subscribe) (*pb.Envelope, func()) {
	b := c.b
	if !b.catalogReady() {
		return failure(req, pb.Status_STATUS_UNAVAILABLE, "catálogo ainda não sincronizado com o par"), nil
	}
	alive := false
	if b.link != nil {
		b.interestMu.Lock()
		defer b.interestMu.Unlock()
		alive = b.link.Link().Alive
		if !alive && !m.GetRestore() {
			return failure(req, pb.Status_STATUS_UNAVAILABLE, "enlace com o par indisponível"), nil
		}
	}

	b.topicMu.RLock()
	info, ok := b.catalog.Lookup(m.GetName())
	if !ok || (m.GetGeneration() != 0 && topic.Generation(m.GetGeneration()) != info.Generation) {
		b.topicMu.RUnlock()
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico inexistente ou geração antiga"), nil
	}
	had := b.subs.HasLocal(info.Ref)
	b.subs.Add(c.session, info)
	if m.GetRestore() {
		b.log.Info("inscrição restaurada", "identidade", c.session.Identity, "sessao", uint64(c.session.ID),
			"topico", info.Name, "geracao", uint32(info.Generation), "enlace_ativo", alive || b.link == nil)
	}
	reply := &pb.Envelope{Body: &pb.Envelope_SubscribeReply{SubscribeReply: &pb.SubscribeReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
	if b.link == nil || had || !alive {
		return reply, b.topicMu.RUnlock
	}
	b.topicMu.RUnlock()

	if err := b.link.Announce(info.Ref, true); err != nil && !m.GetRestore() {
		b.subs.Remove(c.session.ID, info.Ref)
		return failure(req, pb.Status_STATUS_UNAVAILABLE, "par não confirmou a inscrição"), nil
	}
	b.topicMu.RLock()
	if _, ok := b.catalog.Resolve(info.Ref); !ok {
		b.topicMu.RUnlock()
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico apagado durante a inscrição"), nil
	}
	return reply, b.topicMu.RUnlock
}

// unsubscribe é idempotente: remover uma inscrição inexistente responde OK.
// A inscrição sai da tabela local mesmo se o par não confirmar a retirada
// do interesse; nesse caso o enlace cai e o par descarta a visão.
func (c *clientConn) unsubscribe(req *pb.Envelope, m *pb.Unsubscribe) *pb.Envelope {
	b := c.b
	if b.link != nil {
		b.interestMu.Lock()
		defer b.interestMu.Unlock()
	}
	info, ok := b.catalog.Lookup(m.GetName())
	if !ok {
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico não encontrado")
	}
	ref := info.Ref
	if m.GetGeneration() != 0 {
		ref.Generation = topic.Generation(m.GetGeneration())
	}
	if _, removed := b.subs.Remove(c.session.ID, ref); removed && b.link != nil && !b.subs.HasLocal(ref) {
		b.link.Announce(ref, false)
	}
	return &pb.Envelope{Body: &pb.Envelope_UnsubscribeReply{UnsubscribeReply: &pb.UnsubscribeReply{
		Status: pb.Status_STATUS_OK,
	}}}
}

func (c *clientConn) getStats(req *pb.Envelope) *pb.Envelope {
	if !c.session.Admin {
		return failure(req, pb.Status_STATUS_FORBIDDEN, "GetStats restrito a administradores")
	}
	s := c.b.cfg.Stats.Stats()
	return &pb.Envelope{Body: &pb.Envelope_GetStatsReply{GetStatsReply: &pb.GetStatsReply{
		Status:   pb.Status_STATUS_OK,
		BrokerId: c.b.cfg.ID,
		UptimeMs: uint64(time.Since(c.b.started) / time.Millisecond),
		Counters: s.Counters,
		Gauges:   s.Gauges,
	}}}
}

func topicErrorStatus(err error) pb.Status {
	switch {
	case errors.Is(err, topic.ErrTopicExists):
		return pb.Status_STATUS_TOPIC_EXISTS
	case errors.Is(err, topic.ErrTopicNotFound):
		return pb.Status_STATUS_TOPIC_NOT_FOUND
	case errors.Is(err, topic.ErrInvalidName), errors.Is(err, topic.ErrInvalidKind), errors.Is(err, topic.ErrInvalidTTL):
		return pb.Status_STATUS_INVALID_ARGUMENT
	case errors.Is(err, peer.ErrUnavailable):
		return pb.Status_STATUS_UNAVAILABLE
	}
	return pb.Status_STATUS_INTERNAL
}

// failure monta a resposta de erro do tipo correspondente ao comando.
func failure(req *pb.Envelope, st pb.Status, detail string) *pb.Envelope {
	switch req.Body.(type) {
	case *pb.Envelope_CreateTopic:
		return &pb.Envelope{Body: &pb.Envelope_CreateTopicReply{CreateTopicReply: &pb.CreateTopicReply{Status: st, Detail: detail}}}
	case *pb.Envelope_DeleteTopic:
		return &pb.Envelope{Body: &pb.Envelope_DeleteTopicReply{DeleteTopicReply: &pb.DeleteTopicReply{Status: st, Detail: detail}}}
	case *pb.Envelope_LookupTopic:
		return &pb.Envelope{Body: &pb.Envelope_LookupTopicReply{LookupTopicReply: &pb.LookupTopicReply{Status: st, Detail: detail}}}
	case *pb.Envelope_Subscribe:
		return &pb.Envelope{Body: &pb.Envelope_SubscribeReply{SubscribeReply: &pb.SubscribeReply{Status: st, Detail: detail}}}
	case *pb.Envelope_Unsubscribe:
		return &pb.Envelope{Body: &pb.Envelope_UnsubscribeReply{UnsubscribeReply: &pb.UnsubscribeReply{Status: st, Detail: detail}}}
	case *pb.Envelope_GetStats:
		return &pb.Envelope{Body: &pb.Envelope_GetStatsReply{GetStatsReply: &pb.GetStatsReply{Status: st, Detail: detail}}}
	}
	panic("broker: comando sem resposta definida")
}
