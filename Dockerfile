FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG APP_VERSION=
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X github.com/flixermedia/flixer-logger/internal/httpapi.Version=${APP_VERSION}" -o /out/flixer-logger ./cmd/flixer-logger

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates openssl \
    && adduser -D -H -u 65531 logger \
    && mkdir -p /data \
    && chown logger:logger /data

COPY --from=build /out/flixer-logger /usr/local/bin/flixer-logger
COPY scripts/logger-entrypoint.sh /usr/local/bin/logger-entrypoint
RUN chmod 0755 /usr/local/bin/logger-entrypoint
USER logger
EXPOSE 8098
ENTRYPOINT ["/usr/local/bin/logger-entrypoint"]
