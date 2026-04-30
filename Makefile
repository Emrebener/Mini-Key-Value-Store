BINARY := minikv
IMAGE := mini-kv-store
ADDR ?= 0.0.0.0:11211
BENCH_ARGS ?=

.PHONY: fmt test run build docker-build docker-run bench-stack-up bench-stack-down bench

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

run:
	go run ./cmd/minikv -addr $(ADDR)

build:
	go build -o bin/$(BINARY) ./cmd/minikv

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -p 11211:11211 $(IMAGE)

bench-stack-up:
	docker compose up -d --build minikv redis memcached

bench-stack-down:
	docker compose down

bench:
	docker compose run --rm bench $(BENCH_ARGS)
