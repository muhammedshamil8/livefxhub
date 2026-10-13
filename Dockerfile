FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /livefxhub .

FROM alpine:3.20
# ca-certificates for the wss:// feed, wget (busybox) for the healthcheck
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 app \
 && mkdir -p /data && chown app:app /data
COPY --from=build /livefxhub /usr/local/bin/livefxhub
USER app
ENV LISTEN_ADDR=:8080 SNAPSHOT_PATH=/data/snapshot.json
EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
ENTRYPOINT ["livefxhub"]
