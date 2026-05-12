FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download && go mod tidy
COPY main.go ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/connector .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/connector /usr/local/bin/connector
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/connector"]
