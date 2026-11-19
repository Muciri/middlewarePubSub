package contract

import (
	"sync"
	"testing"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func TestAdmissionStatusCoversProto(t *testing.T) {
	all := []AdmissionStatus{
		AdmissionAccepted, AdmissionNoSubscribers, AdmissionTopicNotFound,
		AdmissionLimitExceeded, AdmissionOverloaded, AdmissionInvalidSize,
		AdmissionInvalidTTL, AdmissionUnavailable, AdmissionUnknown,
	}
	defined := len(pb.AdmissionStatus_name) - 1
	if len(all) != defined {
		t.Fatalf("%d constantes para %d valores do proto", len(all), defined)
	}
	seen := make(map[AdmissionStatus]bool)
	for _, s := range all {
		if s == pb.AdmissionStatus_ADMISSION_UNSPECIFIED || seen[s] {
			t.Fatalf("constante repetida ou não especificada: %v", s)
		}
		seen[s] = true
	}
}

func TestMemoryResultSink(t *testing.T) {
	var sink MemoryResultSink
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.Report(AdmissionResult{Session: 1, Topic: topic.Ref{ID: 1, Generation: 1}, Sequence: uint64(i), Status: AdmissionAccepted})
		}()
	}
	wg.Wait()
	if got := len(sink.Results()); got != 20 {
		t.Fatalf("%d resultados registrados, esperado 20", got)
	}
}
