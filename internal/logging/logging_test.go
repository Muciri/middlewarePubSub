package logging

import (
	"bytes"
	"regexp"
	"testing"
)

func TestTimestampInUTCWithMilliseconds(t *testing.T) {
	var out bytes.Buffer
	log, err := New(&out, "text", "info")
	if err != nil {
		t.Fatal(err)
	}
	log.Info("teste")
	if !regexp.MustCompile(`^time=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z level=INFO msg=teste`).Match(out.Bytes()) {
		t.Fatalf("linha = %q", out.String())
	}
}

func TestLevelsAndFormats(t *testing.T) {
	var out bytes.Buffer
	log, _ := New(&out, "json", "warn")
	log.Info("omitido")
	log.Warn("aviso")
	if bytes.Contains(out.Bytes(), []byte("omitido")) || !bytes.Contains(out.Bytes(), []byte(`"msg":"aviso"`)) {
		t.Fatalf("saída = %q", out.String())
	}
	if _, err := New(&out, "xml", "info"); err == nil {
		t.Fatal("formato inválido aceito")
	}
	if _, err := New(&out, "text", "verbose"); err == nil {
		t.Fatal("nível inválido aceito")
	}
}
