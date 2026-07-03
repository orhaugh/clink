# Multi-stage build for the clink operator. No host Go toolchain needed.
FROM golang:1.23 AS build
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
