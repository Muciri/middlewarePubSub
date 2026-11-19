package peer

import (
	"fmt"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func snapshotToProto(s topic.Snapshot) *pb.CatalogSnapshot {
	m := &pb.CatalogSnapshot{Version: s.Version}
	for _, info := range s.Live {
		m.Live = append(m.Live, protoconv.TopicInfoToProto(info))
	}
	for _, t := range s.Deleted {
		m.Deleted = append(m.Deleted, &pb.Tombstone{Name: t.Name, Id: uint32(t.ID), Generation: uint32(t.Generation)})
	}
	return m
}

func snapshotFromProto(m *pb.CatalogSnapshot) (topic.Snapshot, error) {
	s := topic.Snapshot{Version: m.GetVersion()}
	for _, ti := range m.GetLive() {
		info, err := protoconv.TopicInfoFromProto(ti)
		if err != nil {
			return topic.Snapshot{}, err
		}
		s.Live = append(s.Live, info)
	}
	for _, t := range m.GetDeleted() {
		s.Deleted = append(s.Deleted, topic.Tombstone{
			Name: t.GetName(), ID: topic.ID(t.GetId()), Generation: topic.Generation(t.GetGeneration()),
		})
	}
	return s, nil
}

func opToProto(k OpKind) pb.CatalogOp {
	switch k {
	case OpCreate:
		return pb.CatalogOp_CATALOG_OP_CREATE
	case OpDelete:
		return pb.CatalogOp_CATALOG_OP_DELETE
	}
	return pb.CatalogOp_CATALOG_OP_UNSPECIFIED
}

func updateToProto(u Update) *pb.CatalogUpdate {
	return &pb.CatalogUpdate{
		OperationId: u.OperationID,
		Version:     u.Version,
		Op:          opToProto(u.Kind),
		Topic:       protoconv.TopicInfoToProto(u.Topic),
	}
}

func updateFromProto(m *pb.CatalogUpdate) (Update, error) {
	u := Update{OperationID: m.GetOperationId(), Version: m.GetVersion()}
	switch m.GetOp() {
	case pb.CatalogOp_CATALOG_OP_CREATE:
		u.Kind = OpCreate
	case pb.CatalogOp_CATALOG_OP_DELETE:
		u.Kind = OpDelete
	default:
		return Update{}, fmt.Errorf("peer: operação desconhecida %v", m.GetOp())
	}
	info, err := protoconv.TopicInfoFromProto(m.GetTopic())
	if err != nil {
		return Update{}, err
	}
	u.Topic = info
	return u, nil
}

func requestToProto(r Request) *pb.ForwardCatalog {
	m := &pb.ForwardCatalog{OperationId: r.OperationID}
	switch r.Kind {
	case OpCreate:
		m.Op = &pb.ForwardCatalog_Create{Create: protoconv.SpecToProto(r.Spec)}
	case OpDelete:
		m.Op = &pb.ForwardCatalog_Delete{Delete: &pb.DeleteTopic{Name: r.Name}}
	}
	return m
}

func requestFromProto(m *pb.ForwardCatalog) (Request, bool) {
	r := Request{OperationID: m.GetOperationId()}
	switch op := m.Op.(type) {
	case *pb.ForwardCatalog_Create:
		r.Kind = OpCreate
		r.Spec = protoconv.SpecFromProto(op.Create)
	case *pb.ForwardCatalog_Delete:
		r.Kind = OpDelete
		r.Name = op.Delete.GetName()
	default:
		return Request{}, false
	}
	return r, true
}

func refToProto(r topic.Ref) *pb.TopicRef {
	return &pb.TopicRef{Id: uint32(r.ID), Generation: uint32(r.Generation)}
}

func refFromProto(m *pb.TopicRef) topic.Ref {
	return topic.Ref{ID: topic.ID(m.GetId()), Generation: topic.Generation(m.GetGeneration())}
}
