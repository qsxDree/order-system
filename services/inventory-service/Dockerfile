# Build stage
FROM golang:alpine AS builder
WORKDIR /workspace

# Copy shared core library and service code
COPY pkg/ ./pkg/
COPY services/inventory-service/ ./services/inventory-service/

WORKDIR /workspace/services/inventory-service
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/server ./cmd/server

# Runtime stage
FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/server /app/server
EXPOSE 4002
ENTRYPOINT ["/app/server"]
