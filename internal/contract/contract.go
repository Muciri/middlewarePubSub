// Package contract define as interfaces entre o plano de controle (TCP) e o
// plano de dados (UDP), com implementações em memória.
//
//	Sessões                Sessions        SessionTable
//	Registro de tópicos    topic.Registry  topic.Catalog
//	Inscrições locais      Subscriptions   SubscriptionTable
//	Interesse do par       InterestView    MemoryInterest
//	Enlace entre brokers   LinkState       MemoryLink
//	Resultado de admissão  ResultSink      MemoryResultSink
//	Diagnóstico            StatsSource     EmptyStats
//	Cota de comandos       CommandLimiter  AllowAllCommands
//
// Nas cinco primeiras, o plano de dados só lê; as operações de escrita ficam
// nos tipos concretos, usados pelo plano de controle. ResultSink é chamado
// pelo plano de dados e implementado pelo broker. As duas últimas são
// implementadas pelo plano de dados e consultadas pelo broker.
package contract
