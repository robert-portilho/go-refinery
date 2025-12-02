FROM golang:1.25.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -o pipeline cmd/pipeline/main.go
#RUN go build -o benchmark cmd/benchmark/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/pipeline .-o 
#COPY --from=builder /app/benchmark .
COPY configs/config.yaml configs/config.yaml

CMD ["./pipeline"]
