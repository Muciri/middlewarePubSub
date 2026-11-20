package eventwire

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Header{Flags: 3, Topic: 0x01020304, Generation: 0x05060708, Session: 0x0102030405060708, Sequence: 0x1112131415161718, TTL: 500 * time.Millisecond}
	payload := []byte("lance")
	packet, err := Encode(in, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) != HeaderSize+len(payload) {
		t.Fatalf("tamanho = %d", len(packet))
	}
	if packet[0] != Version || binary.BigEndian.Uint32(packet[30:34]) != uint32(len(payload)) {
		t.Fatalf("cabeçalho inesperado: %v", packet[:HeaderSize])
	}
	got, body, err := Decode(packet)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != Version || got.Flags != in.Flags || got.Topic != topic.ID(in.Topic) || got.Generation != in.Generation || got.Session != in.Session || got.Sequence != in.Sequence || got.TTL != in.TTL || string(body) != string(payload) {
		t.Fatalf("round trip = %+v %q", got, body)
	}
}

func TestInvalidPacketSizes(t *testing.T) {
	if _, _, err := Decode(make([]byte, HeaderSize-1)); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("cabeçalho curto: %v", err)
	}
	packet, _ := Encode(Header{TTL: time.Second}, make([]byte, MaxPayload))
	if _, _, err := Decode(packet); err != nil {
		t.Fatalf("payload máximo: %v", err)
	}
	if _, err := Encode(Header{TTL: time.Second}, make([]byte, MaxPayload+1)); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("payload excedente: %v", err)
	}
	packet[33]++
	if _, _, err := Decode(packet); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("comprimento divergente: %v", err)
	}
	packet, _ = Encode(Header{TTL: time.Second}, nil)
	binary.BigEndian.PutUint32(packet[30:34], MaxPayload+1)
	if _, _, err := Decode(packet); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("tamanho declarado excedente: %v", err)
	}
}

func TestInvalidVersionAndTTL(t *testing.T) {
	if _, err := Encode(Header{Version: Version + 1, TTL: time.Second}, nil); !errors.Is(err, ErrVersion) {
		t.Fatalf("versão: %v", err)
	}
	if _, err := Encode(Header{TTL: 0}, nil); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("validade: %v", err)
	}
	p, _ := Encode(Header{TTL: time.Second}, nil)
	p[0]++
	if _, _, err := Decode(p); !errors.Is(err, ErrVersion) {
		t.Fatalf("versão no decode: %v", err)
	}
}
