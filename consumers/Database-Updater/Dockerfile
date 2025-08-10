# Stage 1: Build
FROM golang:1.24.4-alpine AS builder

WORKDIR /app

# Install git
RUN apk add --no-cache git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the binary
RUN go build -o Database-Updater ./cmd/main.go

# Stage 2: Minimal runtime image
FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/Database-Updater .

CMD ["./Database-Updater"]
