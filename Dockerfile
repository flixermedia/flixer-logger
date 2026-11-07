FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/flixer-logger ./cmd/flixer-logger

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 65531 logger \
    && mkdir -p /data \
    && chown logger:logger /data

COPY --from=build /out/flixer-logger /usr/local/bin/flixer-logger
USER logger
EXPOSE 8098
ENTRYPOINT ["/usr/local/bin/flixer-logger"]
