package broker

import (
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/eventwire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

type eventLimiter struct {
	events, bandwidth interface{ Allow(string, int) bool }
}

func newEventLimiter(events, bandwidth interface{ Allow(string, int) bool }) *eventLimiter {
	return &eventLimiter{events, bandwidth}
}
func (l *eventLimiter) AllowEvents(id string, n int) bool {
	return l.events.Allow(id, 1) && l.bandwidth.Allow(id, n)
}

func (b *Broker) serveUDP(conn *net.UDPConn) {
	defer b.wg.Done()
	buf := make([]byte, eventwire.HeaderSize+eventwire.MaxPayload+1)
	for {
		n, from, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			b.count("invalid_datagram")
			if !errors.Is(err, net.ErrClosed) {
				b.log.Warn("leitura UDP", "erro", err)
			}
			return
		}
		b.handleDatagram(conn, from, buf[:n])
	}
}

func (b *Broker) dispatchUDP(conn *net.UDPConn) {
	defer b.wg.Done()
	for {
		select {
		case <-b.stopDispatch:
			return
		case <-b.dispatch:
		case <-time.After(250 * time.Millisecond):
		}
		for {
			_, e, ok := b.scheduler.Next(eventwire.DispatchTurnBytes)
			if !ok {
				break
			}
			left := eventwire.Remaining(e, time.Now())
			if left <= 0 {
				b.count("expired")
				continue
			}
			ref := topic.Ref{ID: e.Header.Topic, Generation: e.Header.Generation}
			_, ok = b.Contracts().Topics.Resolve(ref)
			if !ok {
				b.count("stale_topic")
				continue
			}
			e.Header.TTL = left
			packet, err := eventwire.Encode(e.Header, e.Payload)
			if err != nil {
				b.count("invalid_size")
				continue
			}
			for _, sub := range b.Contracts().Subscriptions.ForTopic(ref) {
				if _, err := conn.WriteToUDPAddrPort(packet, sub.UDPAddr); err != nil {
					b.count("delivery_error")
				}
			}
			b.count("dispatched")
			b.gauge("queue_global", int64(b.scheduler.Len()))
			b.gauge("queue_bytes", int64(b.scheduler.Bytes()))
		}
	}
}

func (b *Broker) count(name string) {
	if s, ok := b.cfg.Stats.(interface{ Inc(string) }); ok {
		s.Inc(name)
	}
}
func (b *Broker) gauge(name string, value int64) {
	if s, ok := b.cfg.Stats.(interface{ SetGauge(string, int64) }); ok {
		s.SetGauge(name, value)
	}
}

func (b *Broker) handleDatagram(conn *net.UDPConn, from netip.AddrPort, packet []byte) {
	h, payload, err := eventwire.Decode(packet)
	if err != nil {
		b.count("invalid_datagram")
		b.log.Debug("datagrama UDP inválido", "erro", err)
		return
	}
	ref := topic.Ref{ID: h.Topic, Generation: h.Generation}
	sessionID := contract.SessionID(h.Session)
	sess, ok := b.Contracts().Sessions.Get(sessionID)
	if !ok || sess.UDPAddr != netip.AddrPortFrom(from.Addr().Unmap(), from.Port()) {
		return
	}
	result := contract.AdmissionResult{Session: sessionID, Topic: ref, Sequence: h.Sequence}
	if !b.cfg.EventLimiter.AllowEvents(sess.Identity, len(packet)) {
		result.Status = contract.AdmissionLimitExceeded
		b.count("rate_limited")
		b.Contracts().Results.Report(result)
		return
	}
	info, ok := b.Contracts().Topics.Resolve(ref)
	switch {
	case !ok:
		result.Status = contract.AdmissionTopicNotFound
		b.count("topic_not_found")
	case !info.AcceptsTTL(h.TTL):
		result.Status = contract.AdmissionInvalidTTL
		b.count("invalid_ttl")
	default:
		subs := b.Contracts().Subscriptions.ForTopic(ref)
		remote := b.Contracts().Interest.RemoteInterest(ref)
		if len(subs) == 0 && !remote.HasSubscribers {
			result.Status = contract.AdmissionNoSubscribers
			b.count("no_subscribers")
		} else {
			e := eventwire.QueuedEvent{Header: h, Payload: append([]byte(nil), payload...), Deadline: time.Now().Add(h.TTL)}
			if !b.scheduler.Enqueue(sess.Identity, e) {
				result.Status = contract.AdmissionOverloaded
				b.count("overloaded")
			} else {
				result.Status = contract.AdmissionAccepted
				b.count("accepted")
				b.gauge("queue_global", int64(b.scheduler.Len()))
				b.gauge("queue_bytes", int64(b.scheduler.Bytes()))
				select {
				case b.dispatch <- struct{}{}:
				default:
				}
			}
		}
	}
	b.Contracts().Results.Report(result)
}
