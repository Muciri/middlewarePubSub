package topic

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestVersionAdvancesOnEveryChange(t *testing.T) {
	c := NewCatalog()
	mustCreate(t, c, Spec{Name: "a", Kind: KindEphemeral})
	mustCreate(t, c, Spec{Name: "b", Kind: KindState})
	c.Delete("a")
	if v := c.Version(); v != 3 {
		t.Fatalf("versão = %d, esperado 3", v)
	}
	c.Create(Spec{Name: "b", Kind: KindState})
	if v := c.Version(); v != 3 {
		t.Fatal("criação recusada não pode avançar a versão")
	}
}

func TestPlanDoesNotChangeCatalog(t *testing.T) {
	c := NewCatalog()
	planned, err := c.PlanCreate(Spec{Name: "sala", Kind: KindEphemeral})
	if err != nil || planned.Ref != (Ref{ID: 1, Generation: 1}) {
		t.Fatalf("PlanCreate = %+v, %v", planned, err)
	}
	if _, ok := c.Lookup("sala"); ok || c.Version() != 0 {
		t.Fatal("PlanCreate alterou o catálogo")
	}
	if _, err := c.PlanDelete("sala"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("PlanDelete de inexistente: %v", err)
	}

	if err := c.Put(planned, 1); err != nil {
		t.Fatal(err)
	}
	if got, err := c.PlanDelete("sala"); err != nil || got != planned {
		t.Fatalf("PlanDelete = %+v, %v", got, err)
	}
	if _, ok := c.Lookup("sala"); !ok {
		t.Fatal("PlanDelete alterou o catálogo")
	}
	if _, err := c.PlanCreate(Spec{Name: "sala", Kind: KindEphemeral}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("PlanCreate de existente: %v", err)
	}
}

func TestPlanCreateAfterDeleteUsesNextGeneration(t *testing.T) {
	c := NewCatalog()
	first := mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	c.Delete("sala")
	planned, err := c.PlanCreate(Spec{Name: "sala", Kind: KindState})
	if err != nil || planned.Ref != (Ref{ID: first.ID, Generation: 2}) || planned.Kind != KindState {
		t.Fatalf("PlanCreate = %+v, %v", planned, err)
	}
}

func TestPutAndRemoveWithExplicitVersion(t *testing.T) {
	c := NewCatalog()
	info := Info{Ref: Ref{ID: 7, Generation: 3}, Name: "sala", Kind: KindState, Policy: DefaultPolicy(KindState)}
	if err := c.Put(info, 10); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Resolve(info.Ref); !ok || got != info || c.Version() != 10 {
		t.Fatalf("Resolve = %+v, %v; versão %d", got, ok, c.Version())
	}
	next := mustCreate(t, c, Spec{Name: "outro", Kind: KindEphemeral})
	if next.ID != 8 {
		t.Fatalf("próximo id = %d, esperado 8", next.ID)
	}

	if _, err := c.Remove(Ref{ID: 7, Generation: 2}, 12); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Remove de geração antiga: %v", err)
	}
	removed, err := c.Remove(info.Ref, 12)
	if err != nil || removed != info || c.Version() != 12 {
		t.Fatalf("Remove = %+v, %v; versão %d", removed, err, c.Version())
	}
	if _, ok := c.Lookup("sala"); ok {
		t.Fatal("tópico removido continua ativo")
	}
}

func TestPutRejectsConflicts(t *testing.T) {
	c := NewCatalog()
	mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	policy := DefaultPolicy(KindEphemeral)
	cases := map[string]Info{
		"nome com outro id": {Ref: Ref{ID: 9, Generation: 1}, Name: "sala", Kind: KindEphemeral, Policy: policy},
		"id com outro nome": {Ref: Ref{ID: 1, Generation: 2}, Name: "outra", Kind: KindEphemeral, Policy: policy},
		"geração zero":      {Ref: Ref{ID: 5}, Name: "nova", Kind: KindEphemeral, Policy: policy},
		"sem validade":      {Ref: Ref{ID: 5, Generation: 1}, Name: "nova", Kind: KindEphemeral},
		"tipo desconhecido": {Ref: Ref{ID: 5, Generation: 1}, Name: "nova", Policy: policy},
		"nome inválido":     {Ref: Ref{ID: 5, Generation: 1}, Name: "Nova", Kind: KindEphemeral, Policy: policy},
	}
	for name, info := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.Put(info, 99); err == nil {
				t.Fatal("esperado erro")
			}
			if c.Version() != 1 {
				t.Fatal("Put recusado alterou a versão")
			}
		})
	}
}

func TestSnapshotRestoreKeepsTombstones(t *testing.T) {
	src := NewCatalog()
	mustCreate(t, src, Spec{Name: "sala-1/lances", Kind: KindEphemeral})
	mustCreate(t, src, Spec{Name: "sala-1/placar", Kind: KindState, MaxTTL: 3 * time.Second})
	src.Delete("sala-1/lances")
	src.Create(Spec{Name: "sala-1/lances", Kind: KindEphemeral})
	src.Delete("sala-1/lances")
	snap := src.Snapshot()

	if snap.Version != 5 || len(snap.Live) != 1 || len(snap.Deleted) != 1 ||
		snap.Deleted[0] != (Tombstone{Name: "sala-1/lances", ID: 1, Generation: 2}) {
		t.Fatalf("snapshot = %+v", snap)
	}

	dst := NewCatalog()
	if _, err := dst.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst.Snapshot(), snap) {
		t.Fatalf("restaurado = %+v", dst.Snapshot())
	}
	again := mustCreate(t, dst, Spec{Name: "sala-1/lances", Kind: KindEphemeral})
	if again.Ref != (Ref{ID: 1, Generation: 3}) {
		t.Fatalf("recriação após restauração = %+v", again.Ref)
	}
	if fresh := mustCreate(t, dst, Spec{Name: "nova", Kind: KindEphemeral}); fresh.ID != 3 {
		t.Fatalf("novo id após restauração = %d, esperado 3", fresh.ID)
	}
}

func TestRestoreReportsSupersededTopics(t *testing.T) {
	c := NewCatalog()
	kept := mustCreate(t, c, Spec{Name: "mantido", Kind: KindEphemeral})
	gone := mustCreate(t, c, Spec{Name: "sumiu", Kind: KindEphemeral})
	old := mustCreate(t, c, Spec{Name: "recriado", Kind: KindState})

	recreated := old
	recreated.Generation = 2
	superseded, err := c.Restore(Snapshot{
		Version: 9,
		Live:    []Info{kept, recreated},
		Deleted: []Tombstone{{Name: "sumiu", ID: gone.ID, Generation: gone.Generation}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(superseded, []Info{old, gone}) {
		t.Fatalf("superados = %+v", superseded)
	}
	if c.Version() != 9 {
		t.Fatalf("versão = %d", c.Version())
	}
}

func TestRestoreRejectsInvalidSnapshot(t *testing.T) {
	c := NewCatalog()
	mustCreate(t, c, Spec{Name: "sala", Kind: KindEphemeral})
	before := c.Snapshot()
	policy := DefaultPolicy(KindEphemeral)
	info := Info{Ref: Ref{ID: 1, Generation: 1}, Name: "a", Kind: KindEphemeral, Policy: policy}

	cases := map[string]Snapshot{
		"nome repetido":    {Live: []Info{info}, Deleted: []Tombstone{{Name: "a", ID: 2, Generation: 1}}},
		"id repetido":      {Live: []Info{info, {Ref: Ref{ID: 1, Generation: 1}, Name: "b", Kind: KindEphemeral, Policy: policy}}},
		"tópico inválido":  {Live: []Info{{Ref: Ref{ID: 1, Generation: 1}, Name: "a"}}},
		"apagado inválido": {Deleted: []Tombstone{{Name: "a", ID: 0, Generation: 1}}},
	}
	for name, snap := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Restore(snap); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("esperado ErrInvalidSnapshot, obtido %v", err)
			}
			if !reflect.DeepEqual(c.Snapshot(), before) {
				t.Fatal("snapshot inválido alterou o catálogo")
			}
		})
	}
}
