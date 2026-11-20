package broker

import (
	"errors"
	"net"
	"net/netip"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/eventwire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func (b *Broker) serveUDP(conn *net.UDPConn) {
	defer b.wg.Done()
	buf := make([]byte, eventwire.HeaderSize+eventwire.MaxPayload+1)
	for {
		n, from, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				b.log.Warn("leitura UDP", "erro", err)
			}
			return
		}
		b.handleDatagram(conn, from, buf[:n])
	}
}

func (b *Broker) handleDatagram(conn *net.UDPConn, from netip.AddrPort, packet []byte) {
	h, _, err := eventwire.Decode(packet)
	if err != nil {
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
	info, ok := b.Contracts().Topics.Resolve(ref)
	switch {
	case !ok:
		result.Status = contract.AdmissionTopicNotFound
	case !info.AcceptsTTL(h.TTL):
		result.Status = contract.AdmissionInvalidTTL
	default:
		subs := b.Contracts().Subscriptions.ForTopic(ref)
		remote := b.Contracts().Interest.RemoteInterest(ref)
		if len(subs) == 0 && !remote.HasSubscribers {
			result.Status = contract.AdmissionNoSubscribers
		} else {
			result.Status = contract.AdmissionAccepted
			if len(subs) > 0 {
				for _, sub := range subs {
					_, _ = conn.WriteToUDPAddrPort(packet, sub.UDPAddr)
				}
			}
		}
	}
	b.Contracts().Results.Report(result)
}
