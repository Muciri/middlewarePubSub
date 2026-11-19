package broker

import (
	"errors"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// dispatch trata uma mensagem após o Hello. Devolve falso para encerrar a
// conexão: violação de protocolo ou falha ao responder.
func (c *clientConn) dispatch(req *pb.Envelope) bool {
	if req.GetHeartbeat() != nil {
		return true
	}
	if !isCommand(req) {
		return false
	}
	var reply *pb.Envelope
	if c.b.cfg.Limiter.AllowCommand(c.session.Identity) {
		reply = c.execute(req)
	} else {
		reply = failure(req, pb.Status_STATUS_LIMIT_EXCEEDED, "cota de comandos excedida")
	}
	reply.RequestId = req.RequestId
	return c.wc.Send(reply) == nil
}

func isCommand(req *pb.Envelope) bool {
	switch req.Body.(type) {
	case *pb.Envelope_CreateTopic, *pb.Envelope_DeleteTopic, *pb.Envelope_LookupTopic,
		*pb.Envelope_Subscribe, *pb.Envelope_Unsubscribe, *pb.Envelope_GetStats:
		return true
	}
	return false
}

func (c *clientConn) execute(req *pb.Envelope) *pb.Envelope {
	switch body := req.Body.(type) {
	case *pb.Envelope_CreateTopic:
		return c.createTopic(req, body.CreateTopic)
	case *pb.Envelope_DeleteTopic:
		return c.deleteTopic(req, body.DeleteTopic)
	case *pb.Envelope_LookupTopic:
		return c.lookupTopic(req, body.LookupTopic)
	case *pb.Envelope_Subscribe:
		return c.subscribe(req, body.Subscribe)
	case *pb.Envelope_Unsubscribe:
		return c.unsubscribe(req, body.Unsubscribe)
	case *pb.Envelope_GetStats:
		return c.getStats(req)
	}
	panic("broker: comando sem tratamento")
}

func (c *clientConn) createTopic(req *pb.Envelope, m *pb.CreateTopic) *pb.Envelope {
	if c.b.cfg.RestrictTopicAdmin && !c.session.Admin {
		return failure(req, pb.Status_STATUS_FORBIDDEN, "criação restrita a administradores")
	}
	c.b.topicMu.Lock()
	info, err := c.b.catalog.Create(protoconv.SpecFromProto(m))
	c.b.topicMu.Unlock()
	if err != nil {
		return failure(req, topicErrorStatus(err), err.Error())
	}
	return &pb.Envelope{Body: &pb.Envelope_CreateTopicReply{CreateTopicReply: &pb.CreateTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
}

func (c *clientConn) deleteTopic(req *pb.Envelope, m *pb.DeleteTopic) *pb.Envelope {
	if c.b.cfg.RestrictTopicAdmin && !c.session.Admin {
		return failure(req, pb.Status_STATUS_FORBIDDEN, "exclusão restrita a administradores")
	}
	info, err := c.b.deleteTopic(m.GetName())
	if err != nil {
		return failure(req, topicErrorStatus(err), err.Error())
	}
	return &pb.Envelope{Body: &pb.Envelope_DeleteTopicReply{DeleteTopicReply: &pb.DeleteTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
}

// deleteTopic apaga o tópico, remove as inscrições locais de todas as
// gerações e avisa as sessões inscritas.
func (b *Broker) deleteTopic(name string) (topic.Info, error) {
	b.topicMu.Lock()
	info, err := b.catalog.Delete(name)
	var removed []contract.Subscription
	if err == nil {
		removed = b.subs.RemoveTopic(info.ID)
	}
	b.topicMu.Unlock()
	if err != nil {
		return topic.Info{}, err
	}

	for _, sub := range removed {
		b.push(sub.Session, &pb.Envelope{Body: &pb.Envelope_SubscriptionEnded{SubscriptionEnded: &pb.SubscriptionEnded{
			Topic:  protoconv.TopicInfoToProto(sub.Topic),
			Reason: pb.Status_STATUS_TOPIC_NOT_FOUND,
		}}})
	}
	return info, nil
}

func (c *clientConn) lookupTopic(req *pb.Envelope, m *pb.LookupTopic) *pb.Envelope {
	info, ok := c.b.catalog.Lookup(m.GetName())
	if !ok {
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico não encontrado")
	}
	return &pb.Envelope{Body: &pb.Envelope_LookupTopicReply{LookupTopicReply: &pb.LookupTopicReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
}

func (c *clientConn) subscribe(req *pb.Envelope, m *pb.Subscribe) *pb.Envelope {
	c.b.topicMu.RLock()
	defer c.b.topicMu.RUnlock()

	info, ok := c.b.catalog.Lookup(m.GetName())
	if !ok || (m.GetGeneration() != 0 && topic.Generation(m.GetGeneration()) != info.Generation) {
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico inexistente ou geração antiga")
	}
	c.b.subs.Add(c.session, info)
	return &pb.Envelope{Body: &pb.Envelope_SubscribeReply{SubscribeReply: &pb.SubscribeReply{
		Status: pb.Status_STATUS_OK, Topic: protoconv.TopicInfoToProto(info),
	}}}
}

// unsubscribe é idempotente: remover uma inscrição inexistente responde OK.
func (c *clientConn) unsubscribe(req *pb.Envelope, m *pb.Unsubscribe) *pb.Envelope {
	info, ok := c.b.catalog.Lookup(m.GetName())
	if !ok {
		return failure(req, pb.Status_STATUS_TOPIC_NOT_FOUND, "tópico não encontrado")
	}
	ref := info.Ref
	if m.GetGeneration() != 0 {
		ref.Generation = topic.Generation(m.GetGeneration())
	}
	c.b.subs.Remove(c.session.ID, ref)
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
