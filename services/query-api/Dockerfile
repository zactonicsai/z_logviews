FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod main.go ./
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/query-api .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/query-api /usr/local/bin/query-api
EXPOSE 8090
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/query-api"]
