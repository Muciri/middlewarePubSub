// Package contract define as interfaces entre o plano de controle (TCP) e o
// plano de dados (UDP), com implementações em memória.
//
//	Sessões                Sessions        SessionTable
//	Registro de tópicos    topic.Registry  topic.Catalog
//	Inscrições locais      Subscriptions   SubscriptionTable
//	Interesse do par       InterestView    MemoryInterest
//	Enlace entre brokers   LinkState       MemoryLink
//	Resultado de admissão  ResultSink      MemoryResultSink
//
// As interfaces expõem só leitura ao plano de dados; as operações de escrita
// ficam nos tipos concretos, usados pelo plano de controle.
package contract
