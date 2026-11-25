// Package cqrs provides lightweight, architecture-neutral Go primitives for Command and Query
// Responsibility Segregation: strongly typed CommandHandler and QueryHandler abstractions,
// composable non-generic middlewares, optional deterministic CommandBus and QueryBus
// dispatchers, context-scoped causation propagation, and jittered retry middleware.
//
// Core cqrs keeps zero dependencies on PostgreSQL drivers, event stores, projection daemons,
// or message brokers so callers remain free to organize applications via layered Clean
// Architecture, vertical slices, or plain handlers.
package cqrs
