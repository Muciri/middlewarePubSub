// Package protoconv converte entre os tipos de topic e as mensagens Protobuf.
package protoconv

import (
	"errors"
	"fmt"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

var ErrInvalidTopicInfo = errors.New("protoconv: TopicInfo inválido")

func KindToProto(k topic.Kind) pb.TopicKind {
	switch k {
	case topic.KindEphemeral:
		return pb.TopicKind_TOPIC_KIND_EPHEMERAL
	case topic.KindState:
		return pb.TopicKind_TOPIC_KIND_STATE
	}
	return pb.TopicKind_TOPIC_KIND_UNSPECIFIED
}

// KindFromProto devolve o tipo zero, inválido, para valores desconhecidos.
func KindFromProto(k pb.TopicKind) topic.Kind {
	switch k {
	case pb.TopicKind_TOPIC_KIND_EPHEMERAL:
		return topic.KindEphemeral
	case pb.TopicKind_TOPIC_KIND_STATE:
		return topic.KindState
	}
	return 0
}

func SpecFromProto(m *pb.CreateTopic) topic.Spec {
	return topic.Spec{
		Name:   m.GetName(),
		Kind:   KindFromProto(m.GetKind()),
		MaxTTL: time.Duration(m.GetMaxTtlMs()) * time.Millisecond,
	}
}

func SpecToProto(s topic.Spec) *pb.CreateTopic {
	return &pb.CreateTopic{
		Name:     s.Name,
		Kind:     KindToProto(s.Kind),
		MaxTtlMs: uint32(s.MaxTTL / time.Millisecond),
	}
}

func TopicInfoToProto(i topic.Info) *pb.TopicInfo {
	return &pb.TopicInfo{
		Id:         uint32(i.ID),
		Name:       i.Name,
		Generation: uint32(i.Generation),
		Kind:       KindToProto(i.Kind),
		MaxTtlMs:   uint32(i.Policy.MaxTTL / time.Millisecond),
	}
}

// TopicInfoFromProto reconstrói a política a partir do tipo, com a validade
// informada pelo broker.
func TopicInfoFromProto(m *pb.TopicInfo) (topic.Info, error) {
	kind := KindFromProto(m.GetKind())
	switch {
	case m == nil:
		return topic.Info{}, fmt.Errorf("%w: ausente", ErrInvalidTopicInfo)
	case m.GetId() == 0 || m.GetGeneration() == 0:
		return topic.Info{}, fmt.Errorf("%w: id ou geração zero", ErrInvalidTopicInfo)
	case !kind.Valid():
		return topic.Info{}, fmt.Errorf("%w: tipo %v", ErrInvalidTopicInfo, m.GetKind())
	case m.GetMaxTtlMs() == 0:
		return topic.Info{}, fmt.Errorf("%w: validade zero", ErrInvalidTopicInfo)
	}
	if err := topic.ValidateName(m.GetName()); err != nil {
		return topic.Info{}, fmt.Errorf("%w: %w", ErrInvalidTopicInfo, err)
	}
	policy := topic.DefaultPolicy(kind)
	policy.MaxTTL = time.Duration(m.GetMaxTtlMs()) * time.Millisecond
	return topic.Info{
		Ref:    topic.Ref{ID: topic.ID(m.GetId()), Generation: topic.Generation(m.GetGeneration())},
		Name:   m.GetName(),
		Kind:   kind,
		Policy: policy,
	}, nil
}
