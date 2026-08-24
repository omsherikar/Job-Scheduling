.PHONY: db-up db-down migrate test build fmt vet check web web-build diagram

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

web:
	cd web && npm install && npm run dev

web-build:
	cd web && npm install && npm run build

diagram:
	go run ./cmd/diagram

check: fmt vet build test
