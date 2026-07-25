# Multi-stage build for the clink operator. No host Go toolchain needed.
#
# Keep this base in step with the `go` directive in go.mod. A base older than
# the directive still builds (GOTOOLCHAIN=auto silently downloads the newer
# toolchain) but only when the build has network, so a stale pin turns into a
# confusing offline failure rather than an honest one.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary so it runs on distroless. Trim symbols for size.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /clink-operator ./cmd

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /clink-operator /clink-operator
USER 65532:65532
ENTRYPOINT ["/clink-operator"]
