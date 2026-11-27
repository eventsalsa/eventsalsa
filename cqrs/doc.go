// Package cqrs provides shared execution observability context and sentinel errors
// for the eventsalsa.dev/cqrs module.
//
// Write-side command handling, dispatchers, boundary error classification, and write
// middlewares live in eventsalsa.dev/cqrs/command. Read-side query handling, typed execution,
// and query middlewares live in eventsalsa.dev/cqrs/query. In-memory Given/When/Then
// domain specifications live in eventsalsa.dev/cqrs/spectest.
//
// Core cqrs keeps zero dependencies on PostgreSQL drivers, event stores, projection daemons,
// or message brokers so callers remain free to structure services cleanly.
package cqrs
