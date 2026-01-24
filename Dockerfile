# Build stage
FROM golang:1.23.2 AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o crypto-stream-engine cmd/server/main.go

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/crypto-stream-engine .
COPY --from=builder /app/web ./web

# Expose ports
EXPOSE 8080

# Run the application
CMD ["./crypto-stream-engine"]