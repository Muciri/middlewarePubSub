package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
)

func TestFrameRoundTrip(t *testing.T) {
	const limit = 1024
	sizes := []int{0, 1, 255, limit}
	var buf bytes.Buffer
	for _, n := range sizes {
		if err := WriteFrame(&buf, bytes.Repeat([]byte{byte(n)}, n), limit); err != nil {
			t.Fatalf("WriteFrame(%d): %v", n, err)
		}
	}
	for _, n := range sizes {
		got, err := ReadFrame(&buf, limit)
		if err != nil {
			t.Fatalf("ReadFrame(%d): %v", n, err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{byte(n)}, n)) {
			t.Fatalf("quadro de %d bytes corrompido", n)
		}
	}
	if _, err := ReadFrame(&buf, limit); err != io.EOF {
		t.Fatalf("fim do fluxo: esperado io.EOF, obtido %v", err)
	}
}

func TestFrameHeaderIsBigEndianLength(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("abc"), 16); err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 3, 'a', 'b', 'c'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("bytes = %v, esperado %v", buf.Bytes(), want)
	}
}

func TestWriteFrameRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	err := WriteFrame(&buf, make([]byte, 11), 10)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("esperado ErrFrameTooLarge, obtido %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("nada deveria ser escrito, escritos %d bytes", buf.Len())
	}
}

func TestReadFrameRejectsForgedLength(t *testing.T) {
	hdr := make([]byte, headerSize)
	binary.BigEndian.PutUint32(hdr, 0xFFFFFFFF)
	r := &countingReader{r: bytes.NewReader(hdr)}
	_, err := ReadFrame(r, MaxClientFrame)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("esperado ErrFrameTooLarge, obtido %v", err)
	}
	if r.n != headerSize {
		t.Fatalf("deveria ler só o cabeçalho, leu %d bytes", r.n)
	}
}

func TestReadFrameTruncated(t *testing.T) {
	cases := map[string][]byte{
		"cabeçalho parcial": {0, 0},
		"conteúdo parcial":  {0, 0, 0, 5, 'a', 'b'},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(data), 16)
			if err != io.ErrUnexpectedEOF {
				t.Fatalf("esperado io.ErrUnexpectedEOF, obtido %v", err)
			}
		})
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := &pb.Envelope{
		RequestId: 42,
		Body: &pb.Envelope_Hello{Hello: &pb.Hello{
			Identity:  "leiloeiro",
			SessionId: 0xDEADBEEFCAFE,
			Token:     "segredo",
			UdpPort:   7001,
		}},
	}
	var buf bytes.Buffer
	if err := WriteMessage(&buf, in, MaxClientFrame); err != nil {
		t.Fatal(err)
	}
	out := &pb.Envelope{}
	if err := ReadMessage(&buf, out, MaxClientFrame); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("mensagem diferente após ida e volta:\n%v\n%v", in, out)
	}
}

func TestReadMessageInvalidPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte{0xFF, 0xFF, 0xFF}, 16); err != nil {
		t.Fatal(err)
	}
	if err := ReadMessage(&buf, &pb.Envelope{}, 16); err == nil {
		t.Fatal("conteúdo inválido deveria falhar")
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}
