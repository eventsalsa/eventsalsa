module eventsalsa.dev/cqrs/examples

go 1.27.4

replace (
	eventsalsa.dev/cqrs => ../
	eventsalsa.dev/cqrs/persistence => ../persistence
	eventsalsa.dev/cqrs/persistence/postgres => ../persistence/postgres
	eventsalsa.dev/store => ../../store
)

require (
	eventsalsa.dev/cqrs v0.0.0-00010101000000-000000000000
	eventsalsa.dev/cqrs/persistence v0.0.0-00010101000000-000000000000
	eventsalsa.dev/cqrs/persistence/postgres v0.0.0-00010101000000-000000000000
	eventsalsa.dev/store v0.4.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.13.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)
