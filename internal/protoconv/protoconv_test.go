package protoconv

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func TestKindRoundTrip(t *testing.T) {
	for _, k := range []topic.Kind{topic.KindEphemeral, topic.KindState} {
		if got := KindFromProto(KindToProto(k)); got != k {
			t.Errorf("%v virou %v", k, got)
		}
	}
	if KindFromProto(pb.TopicKind(77)).Valid() {
		t.Error("tipo desconhecido não pode ser válido")
	}
}

func TestSpecRoundTrip(t *testing.T) {
	spec := topic.Spec{Name: "sala-1/placar", Kind: topic.KindState, MaxTTL: 3 * time.Second}
	if got := SpecFromProto(SpecToProto(spec)); got != spec {
		t.Fatalf("%+v virou %+v", spec, got)
	}
}

func TestTopicInfoRoundTrip(t *testing.T) {
	c := topic.NewCatalog()
	for _, spec := range []topic.Spec{
		{Name: "sala-1/lances", Kind: topic.KindEphemeral},
		{Name: "sala-1/placar", Kind: topic.KindState, MaxTTL: 1500 * time.Millisecond},
	} {
		info, err := c.Create(spec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := TopicInfoFromProto(TopicInfoToProto(info))
		if err != nil {
			t.Fatal(err)
		}
		if got != info {
			t.Fatalf("%+v virou %+v", info, got)
		}
	}
}

func TestTopicInfoFromProtoRejectsInvalid(t *testing.T) {
	valid := &pb.TopicInfo{Id: 1, Name: "sala", Generation: 1, Kind: pb.TopicKind_TOPIC_KIND_STATE, MaxTtlMs: 2000}
	cases := map[string]func(*pb.TopicInfo){
		"id zero":       func(m *pb.TopicInfo) { m.Id = 0 },
		"geração zero":  func(m *pb.TopicInfo) { m.Generation = 0 },
		"sem tipo":      func(m *pb.TopicInfo) { m.Kind = pb.TopicKind_TOPIC_KIND_UNSPECIFIED },
		"validade zero": func(m *pb.TopicInfo) { m.MaxTtlMs = 0 },
		"nome inválido": func(m *pb.TopicInfo) { m.Name = "Sala" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := proto.Clone(valid).(*pb.TopicInfo)
			mutate(m)
			if _, err := TopicInfoFromProto(m); !errors.Is(err, ErrInvalidTopicInfo) {
				t.Fatalf("esperado ErrInvalidTopicInfo, obtido %v", err)
			}
		})
	}
	if _, err := TopicInfoFromProto(nil); !errors.Is(err, ErrInvalidTopicInfo) {
		t.Fatalf("nil: esperado ErrInvalidTopicInfo, obtido %v", err)
	}
}
