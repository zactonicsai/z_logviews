FROM golang:1.25-alpine AS builder
WORKDIR /src
# Copy source FIRST so go mod tidy has something to analyze.
# (Running tidy before source-copy was the bug: it stripped deps from go.mod,
#  then the second tidy fetched latest versions instead of the pinned ones.)
COPY go.mod main.go ./
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sink .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/sink /usr/local/bin/sink
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/sink"]
