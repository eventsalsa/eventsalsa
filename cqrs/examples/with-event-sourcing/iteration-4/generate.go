package main

//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative commands/place_order.proto
//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative commands/add_order_item.proto
//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative commands/confirm_order.proto
//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative queries/get_order_overview.proto
//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative queries/order_overview.proto
