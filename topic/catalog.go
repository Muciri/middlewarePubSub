package topic

import (
	"errors"
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
)

// Catalog mantém os tópicos em memória. O id é fixo por nome: ao apagar, o
// registro permanece com a última geração, e recriar o nome incrementa a
// geração mantendo o id.
type Catalog struct {
	mu     sync.RWMutex
	byName map[string]*record
	byID   map[ID]*record
	nextID ID
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

func (c *Catalog) Create(spec Spec) (Info, error) {
	if err := spec.Validate(); err != nil {
		return Info{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.byName[spec.Name]
	switch {
	case ok && rec.live:
		return Info{}, ErrTopicExists
	case ok:
		if rec.info.Generation == math.MaxUint32 {
			return Info{}, ErrGenerationExhausted
		}
		rec.info.Generation++
	default:
		rec = &record{info: Info{Ref: Ref{ID: c.nextID, Generation: 1}, Name: spec.Name}}
		c.nextID++
		c.byName[spec.Name] = rec
		c.byID[rec.info.ID] = rec
	}
	rec.info.Kind = spec.Kind
	rec.info.Policy = spec.Policy()
	rec.live = true
	return rec.info, nil
}

// Delete apaga o tópico e devolve a versão removida.
func (c *Catalog) Delete(name string) (Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.byName[name]
	if !ok || !rec.live {
		return Info{}, ErrTopicNotFound
	}
	rec.live = false
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
