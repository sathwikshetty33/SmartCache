# Stage 1: Build
FROM golang:1.24.4-alpine  AS builder

WORKDIR /app

# Install git (required for go get)
RUN apk add --no-cache git

# Copy go.mod and go.sum to cache deps
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the binary
RUN go build -o database .

# Stage 2: Minimal image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /app/database .

# Run binary
CMD ["./database"]
