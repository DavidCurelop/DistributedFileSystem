# syntax=docker/dockerfile:1

FROM golang:alpine AS builder
WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET=controlnode
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build -trimpath -ldflags="-w -s" -o /bin/dfs-service ./cmd/${TARGET}


FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

RUN dd if=/dev/urandom of=/test.bin bs=1M count=10

COPY --from=builder /bin/dfs-service /app/dfs-service

ENTRYPOINT ["/app/dfs-service"]