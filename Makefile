.PHONY: build test docker-build docker-up docker-down clean

# Build the application
build:
	go build -o verifiable-postgres-proxy ./cmd/proxy

# Run tests
test:
	go test -v ./...

# Run tests with integration tests enabled
test-integration:
	ENABLE_INTEGRATION_TESTS=1 go test -v ./...

# Build Docker image
docker-build:
	docker build -t verifiable-postgres-proxy .

# Start Docker Compose stack
docker-up:
	docker-compose up -d

# Stop Docker Compose stack
docker-down:
	docker-compose down

# Start Docker Compose stack with logs
docker-up-logs:
	docker-compose up

# Clean build artifacts
clean:
	rm -f verifiable-postgres-proxy
	
# Download dependencies
deps:
	go mod download

# Run the application
run: build
	./verifiable-postgres-proxy