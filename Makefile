.PHONY: db-up db-down migrate test build fmt vet check

db-up:
	docker compose up -d postgres
	until docker compose exec -T postgres pg_isready -U fenceline -d fenceline >/dev/null 2>&1; do sleep 0.5; done

db-down:
	docker compose down

db-reset:
	docker compose down -v
	$(MAKE) db-up
	$(MAKE) migrate

migrate:
	go run ./cmd/migrate

build:
	go build ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test -race ./...

check: fmt vet build test
