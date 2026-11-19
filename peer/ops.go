package peer

import (
	"errors"
	"fmt"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

var errNotAdmin = errors.New("peer: operação exclusiva do broker administrativo")

// Replicate envia a alteração ao par e espera o ACK. Só no broker
// administrativo. Sem confirmação no prazo, o enlace é derrubado: o par
// pode ter aplicado a alteração, e o snapshot da reconexão desfaz a
// diferença, já que o chamador não a aplica localmente.
func (l *Link) Replicate(u Update) error {
	if !l.cfg.Admin {
		return errNotAdmin
	}
	s, err := l.readySession()
	if err != nil {
		return err
	}
	ch := make(chan *pb.CatalogAck, 1)
	s.mu.Lock()
	s.catalogAcks[u.OperationID] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.catalogAcks, u.OperationID)
		s.mu.Unlock()
	}()

	if err := s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_CatalogUpdate{CatalogUpdate: updateToProto(u)}}); err != nil {
		return s.fail(fmt.Errorf("%w: %w", ErrUnavailable, err))
	}
	timer := time.NewTimer(l.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case ack := <-ch:
		if ack.GetStatus() == pb.Status_STATUS_OK && ack.GetVersion() == u.Version {
			return nil
		}
		return s.fail(fmt.Errorf("%w: par recusou a versão %d: %s", ErrUnavailable, u.Version, ack.GetDetail()))
	case <-timer.C:
		return s.fail(fmt.Errorf("%w: sem ACK em %v", ErrUnavailable, l.cfg.AckTimeout))
	case <-s.done:
		return ErrUnavailable
	}
}

// Forward repassa o pedido ao broker administrativo e espera a resposta.
// Quando ela chega, a alteração já foi aplicada localmente pelo Update que
// a precede no mesmo fluxo.
func (l *Link) Forward(req Request) (Result, error) {
	if l.cfg.Admin {
		return Result{}, errors.New("peer: broker administrativo não repassa pedidos")
	}
	s, err := l.readySession()
	if err != nil {
		return Result{}, err
	}
	ch := make(chan *pb.ForwardCatalogReply, 1)
	s.mu.Lock()
	s.forwards[req.OperationID] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.forwards, req.OperationID)
		s.mu.Unlock()
	}()

	if err := s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_ForwardCatalog{ForwardCatalog: requestToProto(req)}}); err != nil {
		return Result{}, s.fail(fmt.Errorf("%w: %w", ErrUnavailable, err))
	}
	timer := time.NewTimer(2 * l.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case reply := <-ch:
		res := Result{Status: reply.GetStatus(), Detail: reply.GetDetail()}
		if res.Status == pb.Status_STATUS_OK {
			info, err := protoconv.TopicInfoFromProto(reply.GetTopic())
			if err != nil {
				return Result{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
			}
			res.Topic = info
		}
		return res, nil
	case <-timer.C:
		return Result{}, fmt.Errorf("%w: sem resposta em %v", ErrUnavailable, 2*l.cfg.AckTimeout)
	case <-s.done:
		return Result{}, ErrUnavailable
	}
}

// Announce informa ao par que a versão de tópico passou a ter ou deixou de
// ter assinantes locais, e espera o ACK. Sem ACK no prazo, o enlace é
// derrubado e a visão completa é reenviada na reconexão.
func (l *Link) Announce(ref topic.Ref, has bool) error {
	s, err := l.readySession()
	if err != nil {
		return err
	}
	ch := make(chan struct{})
	s.interestMu.Lock()
	s.outSeq++
	seq := s.outSeq
	s.mu.Lock()
	s.interestAcks[seq] = ch
	s.mu.Unlock()
	err = s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_InterestUpdate{InterestUpdate: &pb.InterestUpdate{
		Seq: seq, Topic: refToProto(ref), HasSubscribers: has,
	}}})
	s.interestMu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.interestAcks, seq)
		s.mu.Unlock()
	}()
	if err != nil {
		return s.fail(fmt.Errorf("%w: %w", ErrUnavailable, err))
	}

	timer := time.NewTimer(l.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return s.fail(fmt.Errorf("%w: sem ACK de interesse em %v", ErrUnavailable, l.cfg.AckTimeout))
	case <-s.done:
		return ErrUnavailable
	}
}
