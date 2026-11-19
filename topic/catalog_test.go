package topic

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustCreate(t *testing.T, c *Catalog, spec Spec) Info {
	t.Helper()
	info, err := c.Create(spec)
	if err != nil {
		t.Fatalf("Create(%+v): %v", spec, err)
	}
	return info
}

func TestCreateAssignsSequentialIDsAndFirstGeneration(t *testing.T) {
	c := NewCatalog()
	a := mustCreate(t, c, Spec{Name: "sala-1/lances", Kind: KindEphemeral})
	b := mustCreate(t, c, Spec{Name: "sala-1/placar", Kind: KindState})

	if a.Ref != (Ref{ID: 1, Generation: 1}) || b.Ref != (Ref{ID: 2, Generation: 1}) {
		t.Fatalf("refs = %+v e %+v", a.Ref, b.Ref)
	}
	if a.Policy != DefaultPolicy(KindEphemeral) || b.Policy != DefaultPolicy(KindState) {
		t.Fatalf("políticas não são as padrão: %+v, %+v", a.Policy, b.Policy)
	}
}

func TestCreateExistingFails(t *testing.T) {
	c := NewCatalog()
	mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	if _, err := c.Create(Spec{Name: "sala", Kind: KindState}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("esperado ErrTopicExists, obtido %v", err)
	}
}

func TestCreateRejectsInvalidSpec(t *testing.T) {
	c := NewCatalog()
	if _, err := c.Create(Spec{Name: "Sala", Kind: KindEphemeral}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("esperado ErrInvalidName, obtido %v", err)
	}
	if len(c.List()) != 0 {
		t.Fatal("pedido inválido não pode alterar o catálogo")
	}
}

func TestRecreateKeepsIDAndIncrementsGeneration(t *testing.T) {
	c := NewCatalog()
	first := mustCreate(t, c, Spec{Name: "sala-1/placar", Kind: KindState})
	mustCreate(t, c, Spec{Name: "outro", Kind: KindEphemeral})

	deleted, err := c.Delete("sala-1/placar")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != first {
		t.Fatalf("Delete devolveu %+v, esperado %+v", deleted, first)
	}

	second := mustCreate(t, c, Spec{Name: "sala-1/placar", Kind: KindEphemeral, MaxTTL: time.Second})
	if second.ID != first.ID || second.Generation != first.Generation+1 {
		t.Fatalf("recriado com ref %+v a partir de %+v", second.Ref, first.Ref)
	}
	if second.Kind != KindEphemeral || second.Policy.MaxTTL != time.Second {
		t.Fatalf("recriação deve usar o novo pedido: %+v", second)
	}

	if _, ok := c.Resolve(first.Ref); ok {
		t.Fatal("geração antiga não pode ser resolvida")
	}
	if got, ok := c.Resolve(second.Ref); !ok || got != second {
		t.Fatalf("Resolve(nova) = %+v, %v", got, ok)
	}
}

func TestGenerationKeepsGrowingAcrossCycles(t *testing.T) {
	c := NewCatalog()
	for want := Generation(1); want <= 5; want++ {
		info := mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
		if info.ID != 1 || info.Generation != want {
			t.Fatalf("ciclo %d: ref %+v", want, info.Ref)
		}
		if _, err := c.Delete("sala"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeleteMissing(t *testing.T) {
	c := NewCatalog()
	if _, err := c.Delete("nada"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("esperado ErrTopicNotFound, obtido %v", err)
	}
	mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	if _, err := c.Delete("sala"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Delete("sala"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("segunda exclusão: esperado ErrTopicNotFound, obtido %v", err)
	}
}

func TestDeletedTopicIsInvisible(t *testing.T) {
	c := NewCatalog()
	info := mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	mustCreate(t, c, Spec{Name: "b", Kind: KindState})
	mustCreate(t, c, Spec{Name: "a", Kind: KindState})
	if _, err := c.Delete("sala"); err != nil {
		t.Fatal(err)
	}

	if _, ok := c.Lookup("sala"); ok {
		t.Fatal("Lookup encontrou tópico apagado")
	}
	if _, ok := c.Resolve(info.Ref); ok {
		t.Fatal("Resolve encontrou tópico apagado")
	}
	list := c.List()
	if len(list) != 2 || list[0].Name != "a" || list[1].Name != "b" {
		t.Fatalf("List() = %+v", list)
	}
}

func TestResolveUnknownID(t *testing.T) {
	c := NewCatalog()
	if _, ok := c.Resolve(Ref{ID: 99, Generation: 1}); ok {
		t.Fatal("id inexistente não pode ser resolvido")
	}
}

func TestConcurrentCreateSameNameHasSingleWinner(t *testing.T) {
	c := NewCatalog()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Create(Spec{Name: "sala", Kind: KindEphemeral})
			switch {
			case err == nil:
				wins.Add(1)
			case !errors.Is(err, ErrTopicExists):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d criações bem-sucedidas, esperado 1", wins.Load())
	}
}

func TestConcurrentCreateDistinctNamesGetDistinctIDs(t *testing.T) {
	c := NewCatalog()
	const n = 64
	ids := make([]ID, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := c.Create(Spec{Name: fmt.Sprintf("t%d", i), Kind: KindEphemeral})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = info.ID
		}()
	}
	wg.Wait()

	seen := make(map[ID]bool)
	for _, id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("id %d repetido ou zero", id)
		}
		seen[id] = true
	}
}
