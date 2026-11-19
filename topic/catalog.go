package topic

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Registry é a consulta ao catálogo usada pelo plano de dados.
type Registry interface {
	// Lookup devolve o tópico ativo com o nome dado.
	Lookup(name string) (Info, bool)
	// Resolve devolve o tópico ativo cujo id e geração coincidem com ref.
	// Falso indica tópico inexistente ou geração antiga (TOPIC_NOT_FOUND).
	Resolve(ref Ref) (Info, bool)
	// List devolve os tópicos ativos ordenados por nome.
	List() []Info
}

var (
	ErrTopicExists         = errors.New("topic: tópico já existe")
	ErrTopicNotFound       = errors.New("topic: tópico não encontrado")
	ErrGenerationExhausted = errors.New("topic: gerações esgotadas para o nome")
	ErrConflict            = errors.New("topic: nome e identificador incompatíveis com o catálogo")
	ErrInvalidSnapshot     = errors.New("topic: snapshot inválido")
)

// Tombstone guarda a última geração de um nome apagado.
type Tombstone struct {
	Name       string
	ID         ID
	Generation Generation
}

// Snapshot é o catálogo completo numa versão, incluindo os nomes apagados.
type Snapshot struct {
	Version uint64
	Live    []Info
	Deleted []Tombstone
}

// Catalog mantém os tópicos em memória. O id é fixo por nome: ao apagar, o
// registro permanece com a última geração, e recriar o nome incrementa a
// geração mantendo o id. Cada alteração avança a versão do catálogo.
//
// Create e Delete alteram o catálogo diretamente. No broker administrativo,
// PlanCreate e PlanDelete calculam a alteração sem aplicá-la, para que ela
// seja replicada antes; Put e Remove aplicam uma alteração já decidida.
type Catalog struct {
	mu      sync.RWMutex
	byName  map[string]*record
	byID    map[ID]*record
	nextID  ID
	version uint64
}

type record struct {
	info Info
	live bool
}

var _ Registry = (*Catalog)(nil)

func NewCatalog() *Catalog {
	return &Catalog{
		byName: make(map[string]*record),
		byID:   make(map[ID]*record),
		nextID: 1,
	}
}

func (c *Catalog) Version() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

func (c *Catalog) Create(spec Spec) (Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, err := c.planCreateLocked(spec)
	if err != nil {
		return Info{}, err
	}
	c.putLocked(info)
	c.version++
	return info, nil
}

// Delete apaga o tópico e devolve a versão removida.
func (c *Catalog) Delete(name string) (Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, err := c.planDeleteLocked(name)
	if err != nil {
		return Info{}, err
	}
	c.byName[name].live = false
	c.version++
	return info, nil
}

// PlanCreate devolve o tópico que Create produziria, sem alterar o catálogo.
func (c *Catalog) PlanCreate(spec Spec) (Info, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.planCreateLocked(spec)
}

// PlanDelete devolve o tópico que Delete removeria, sem alterar o catálogo.
func (c *Catalog) PlanDelete(name string) (Info, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.planDeleteLocked(name)
}

func (c *Catalog) planCreateLocked(spec Spec) (Info, error) {
	if err := spec.Validate(); err != nil {
		return Info{}, err
	}
	info := Info{Name: spec.Name, Kind: spec.Kind, Policy: spec.Policy()}
	rec, ok := c.byName[spec.Name]
	switch {
	case ok && rec.live:
		return Info{}, ErrTopicExists
	case ok:
		if rec.info.Generation == math.MaxUint32 {
			return Info{}, ErrGenerationExhausted
		}
		info.Ref = Ref{ID: rec.info.ID, Generation: rec.info.Generation + 1}
	default:
		info.Ref = Ref{ID: c.nextID, Generation: 1}
	}
	return info, nil
}

func (c *Catalog) planDeleteLocked(name string) (Info, error) {
	rec, ok := c.byName[name]
	if !ok || !rec.live {
		return Info{}, ErrTopicNotFound
	}
	return rec.info, nil
}

// Put torna info o tópico ativo do seu nome e registra a versão do catálogo.
func (c *Catalog) Put(info Info, version uint64) error {
	if err := validInfo(info); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if rec, ok := c.byName[info.Name]; ok && rec.info.ID != info.ID {
		return fmt.Errorf("%w: %q já tem id %d", ErrConflict, info.Name, rec.info.ID)
	}
	if rec, ok := c.byID[info.ID]; ok && rec.info.Name != info.Name {
		return fmt.Errorf("%w: id %d já pertence a %q", ErrConflict, info.ID, rec.info.Name)
	}
	c.putLocked(info)
	c.version = version
	return nil
}

func (c *Catalog) putLocked(info Info) {
	rec, ok := c.byName[info.Name]
	if !ok {
		rec = &record{}
		c.byName[info.Name] = rec
		c.byID[info.ID] = rec
	}
	rec.info = info
	rec.live = true
	if info.ID >= c.nextID {
		c.nextID = info.ID + 1
	}
}

// Remove apaga a versão ref do tópico e registra a versão do catálogo.
func (c *Catalog) Remove(ref Ref, version uint64) (Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.byID[ref.ID]
	if !ok || !rec.live || rec.info.Generation != ref.Generation {
		return Info{}, ErrTopicNotFound
	}
	rec.live = false
	c.version = version
	return rec.info, nil
}

func (c *Catalog) Lookup(name string) (Info, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rec, ok := c.byName[name]
	if !ok || !rec.live {
		return Info{}, false
	}
	return rec.info, true
}

func (c *Catalog) Resolve(ref Ref) (Info, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rec, ok := c.byID[ref.ID]
	if !ok || !rec.live || rec.info.Generation != ref.Generation {
		return Info{}, false
	}
	return rec.info, true
}

func (c *Catalog) List() []Info {
	c.mu.RLock()
	out := make([]Info, 0, len(c.byName))
	for _, rec := range c.byName {
		if rec.live {
			out = append(out, rec.info)
		}
	}
	c.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (c *Catalog) Snapshot() Snapshot {
	c.mu.RLock()
	s := Snapshot{Version: c.version}
	for _, rec := range c.byName {
		if rec.live {
			s.Live = append(s.Live, rec.info)
		} else {
			s.Deleted = append(s.Deleted, Tombstone{Name: rec.info.Name, ID: rec.info.ID, Generation: rec.info.Generation})
		}
	}
	c.mu.RUnlock()

	sort.Slice(s.Live, func(i, j int) bool { return s.Live[i].Name < s.Live[j].Name })
	sort.Slice(s.Deleted, func(i, j int) bool { return s.Deleted[i].Name < s.Deleted[j].Name })
	return s
}

// Restore substitui o catálogo pelo snapshot. Devolve os tópicos que
// estavam ativos e deixaram de estar com a mesma geração, cujas inscrições
// devem ser encerradas. Um snapshot inválido não altera o catálogo.
func (c *Catalog) Restore(s Snapshot) ([]Info, error) {
	byName := make(map[string]*record, len(s.Live)+len(s.Deleted))
	byID := make(map[ID]*record, len(s.Live)+len(s.Deleted))
	nextID := ID(1)
	add := func(rec *record) error {
		if _, dup := byName[rec.info.Name]; dup {
			return fmt.Errorf("%w: nome %q repetido", ErrInvalidSnapshot, rec.info.Name)
		}
		if _, dup := byID[rec.info.ID]; dup {
			return fmt.Errorf("%w: id %d repetido", ErrInvalidSnapshot, rec.info.ID)
		}
		byName[rec.info.Name] = rec
		byID[rec.info.ID] = rec
		if rec.info.ID >= nextID {
			nextID = rec.info.ID + 1
		}
		return nil
	}
	for _, info := range s.Live {
		if err := validInfo(info); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidSnapshot, err)
		}
		if err := add(&record{info: info, live: true}); err != nil {
			return nil, err
		}
	}
	for _, t := range s.Deleted {
		if ValidateName(t.Name) != nil || t.ID == 0 || t.Generation == 0 {
			return nil, fmt.Errorf("%w: nome apagado inválido %+v", ErrInvalidSnapshot, t)
		}
		info := Info{Ref: Ref{ID: t.ID, Generation: t.Generation}, Name: t.Name}
		if err := add(&record{info: info}); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	var superseded []Info
	for name, old := range c.byName {
		if !old.live {
			continue
		}
		if cur, ok := byName[name]; !ok || !cur.live || cur.info.Ref != old.info.Ref {
			superseded = append(superseded, old.info)
		}
	}
	c.byName, c.byID, c.nextID, c.version = byName, byID, nextID, s.Version
	sort.Slice(superseded, func(i, j int) bool { return superseded[i].Name < superseded[j].Name })
	return superseded, nil
}

func validInfo(info Info) error {
	switch {
	case ValidateName(info.Name) != nil:
		return fmt.Errorf("%w: %q", ErrInvalidName, info.Name)
	case !info.Kind.Valid():
		return fmt.Errorf("%w: %v", ErrInvalidKind, info.Kind)
	case info.ID == 0 || info.Generation == 0:
		return fmt.Errorf("%w: id ou geração zero", ErrConflict)
	case info.Policy.MaxTTL <= 0:
		return fmt.Errorf("%w: %v", ErrInvalidTTL, info.Policy.MaxTTL)
	}
	return nil
}
