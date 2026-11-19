package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// Reconnect descreve uma troca de broker concluída.
type Reconnect struct {
	From, To string
	// DetectedAt é o instante da detecção da falha; RestoredAt, o fim da
	// restauração das inscrições.
	DetectedAt time.Time
	RestoredAt time.Time
	Attempts   int
	// Restored conta as inscrições restauradas; Ended, as encerradas por
	// tópico apagado ou recriado.
	Restored int
	Ended    int
}

// Gap é o tempo entre a detecção da falha e a restauração das inscrições.
func (r Reconnect) Gap() time.Duration { return r.RestoredAt.Sub(r.DetectedAt) }

// errRestoreUnavailable indica broker que ainda não pode restaurar as
// inscrições; a tentativa passa ao broker seguinte.
var errRestoreUnavailable = errors.New("client: broker sem catálogo sincronizado")

// connLost inicia a reconexão quando o canal atual cai. Canais de
// tentativas em curso, ou já substituídos, são ignorados.
func (c *Client) connLost(conn *controlConn) {
	c.mu.Lock()
	if c.err != nil || c.reconnecting || c.conn != conn {
		c.mu.Unlock()
		return
	}
	detected := time.Now()
	c.reconnecting = true
	c.epoch++
	// O catálogo do próximo broker é a referência daqui em diante.
	c.topics = make(map[string]topic.Info)
	from := c.current
	c.wg.Add(1)
	c.mu.Unlock()

	conn.close()
	c.pending.resolveAll(StatusUnknown)
	c.log.Warn("client: falha no canal de controle",
		"broker", conn.brokerID, "addr", c.cfg.Brokers[from], "causa", conn.cause())

	go func() {
		r, ok := c.reconnect(from, conn.brokerID, detected)
		c.wg.Done()
		if ok && c.cfg.OnReconnect != nil {
			c.cfg.OnReconnect(r)
		}
	}()
}

// reconnect tenta os brokers a partir do seguinte ao que falhou, com recuo
// progressivo entre as tentativas, até restaurar as inscrições, esgotar
// ReconnectTimeout ou o cliente ser encerrado. Não volta ao broker
// anterior enquanto o atual funcionar. Devolve falso se não reconectou.
func (c *Client) reconnect(from int, fromID string, detected time.Time) (Reconnect, bool) {
	n := len(c.cfg.Brokers)
	backoff := c.cfg.ReconnectBackoffMin
	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			select {
			case <-c.closing.Done():
				return Reconnect{}, false
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, c.cfg.ReconnectBackoffMax)
		}
		if c.cfg.ReconnectTimeout > 0 && time.Since(detected) > c.cfg.ReconnectTimeout {
			c.log.Error("client: reconexão desistiu", "desde_falha", time.Since(detected), "tentativas", attempt-1)
			c.shutdown(fmt.Errorf("%w: nenhum broker respondeu em %v", ErrConnectionLost, c.cfg.ReconnectTimeout))
			return Reconnect{}, false
		}

		idx := (from + attempt) % n
		addr := c.cfg.Brokers[idx]
		conn, err := c.dialAttempt(addr)
		if err != nil {
			if c.closing.Err() != nil {
				return Reconnect{}, false
			}
			if terminalDialError(err) {
				c.log.Error("client: broker recusou a sessão na reconexão", "addr", addr, "erro", err)
				c.shutdown(err)
				return Reconnect{}, false
			}
			c.log.Debug("client: tentativa de reconexão falhou", "addr", addr, "erro", err)
			continue
		}

		restored, ended, err := c.restore(conn)
		if err == nil {
			c.dp.SetBroker(conn.udpAddr)
			err = c.install(conn, idx)
		}
		if err != nil {
			conn.close()
			if c.closing.Err() != nil {
				return Reconnect{}, false
			}
			c.log.Debug("client: restauração falhou", "addr", addr, "erro", err)
			continue
		}

		r := Reconnect{
			From: fromID, To: conn.brokerID,
			DetectedAt: detected, RestoredAt: time.Now(),
			Attempts: attempt, Restored: len(restored), Ended: ended,
		}
		c.dropCancelled(conn, restored)
		c.log.Info("client: inscrições restauradas",
			"de", r.From, "para", r.To, "lacuna", r.Gap(),
			"tentativas", r.Attempts, "restauradas", r.Restored, "encerradas", r.Ended)
		return r, true
	}
}

func (c *Client) dialAttempt(addr string) (*controlConn, error) {
	ctx, cancel := context.WithTimeout(c.closing, c.cfg.HeartbeatTimeout)
	defer cancel()
	conn, err := dialControl(ctx, addr, c.cfg, c.session, c.dp.Port())
	if err != nil {
		return nil, err
	}
	c.startConn(conn)
	return conn, nil
}

// restore reenvia as inscrições ativas ao novo broker com a geração
// conhecida. Inscrições de tópico apagado ou recriado terminam com erro;
// falha do canal ou broker sem catálogo encerram a tentativa, e as
// inscrições já restauradas nela são refeitas no próximo broker.
func (c *Client) restore(conn *controlConn) (restored []*Subscription, ended int, err error) {
	c.mu.Lock()
	subs := make([]*Subscription, 0, len(c.byRef))
	for _, s := range c.byRef {
		subs = append(subs, s)
	}
	c.mu.Unlock()

	for _, s := range subs {
		if s.isEnded() {
			continue
		}
		env, err := c.restoreCall(conn, &pb.Envelope{Body: &pb.Envelope_Subscribe{Subscribe: &pb.Subscribe{
			Name: s.info.Name, Generation: uint32(s.info.Generation), Restore: true,
		}}})
		if err != nil {
			return restored, ended, err
		}
		reply := env.GetSubscribeReply()
		switch reply.GetStatus() {
		case pb.Status_STATUS_OK:
			restored = append(restored, s)
		case pb.Status_STATUS_UNAVAILABLE:
			return restored, ended, errRestoreUnavailable
		case pb.Status_STATUS_TOPIC_NOT_FOUND:
			ended++
			c.forgetTopic(s.info.Ref)
			c.endSubscription(s, c.vanished(conn, s.info))
		default:
			ended++
			c.endSubscription(s, statusError(reply.GetStatus(), reply.GetDetail()))
		}
	}
	return restored, ended, nil
}

func (c *Client) restoreCall(conn *controlConn, env *pb.Envelope) (*pb.Envelope, error) {
	ctx, cancel := context.WithTimeout(c.closing, c.cfg.HeartbeatTimeout)
	defer cancel()
	return conn.call(ctx, env)
}

// vanished distingue tópico apagado de tópico recriado com outra geração.
func (c *Client) vanished(conn *controlConn, info topic.Info) error {
	env, err := c.restoreCall(conn, &pb.Envelope{Body: &pb.Envelope_LookupTopic{LookupTopic: &pb.LookupTopic{Name: info.Name}}})
	if err == nil {
		reply := env.GetLookupTopicReply()
		if reply.GetStatus() == pb.Status_STATUS_OK && topic.Generation(reply.GetTopic().GetGeneration()) != info.Generation {
			return ErrTopicRecreated
		}
	}
	return ErrTopicDeleted
}

// dropCancelled retira do broker as inscrições restauradas que a aplicação
// cancelou durante a reconexão, quando o cancelamento foi só local.
func (c *Client) dropCancelled(conn *controlConn, restored []*Subscription) {
	for _, s := range restored {
		if s.isEnded() {
			c.restoreCall(conn, unsubscribeMsg(s.info))
		}
	}
}

// install torna conn o canal atual. Se ele caiu durante a restauração, a
// tentativa falha e a reconexão continua.
func (c *Client) install(conn *controlConn, idx int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if err := conn.cause(); err != nil {
		return err
	}
	c.conn = conn
	c.current = idx
	c.reconnecting = false
	return nil
}
