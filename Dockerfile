FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod .

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o verifiable-postgres-proxy ./cmd/proxy

# Use a minimal alpine image for the final image
FROM alpine:3.19

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/verifiable-postgres-proxy /app/verifiable-postgres-proxy

# Expose the proxy port
EXPOSE 5432

# Run the application
ENTRYPOINT ["/app/verifiable-postgres-proxy"]