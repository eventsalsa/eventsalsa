// Package migrations provides SQL migration generation.
//
// To generate migrations, use the migrate-gen command:
//
//	go run eventsalsa.dev/store/cmd/migrate-gen -output migrations
//
// Or add a go generate directive to your code:
//
//	//go:generate go run eventsalsa.dev/store/cmd/migrate-gen -output ../../migrations
//
// Then run:
//
//	go generate ./...
package migrations
