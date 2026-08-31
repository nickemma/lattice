FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/lattice ./cmd && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/lattice-indexer ./cmd/indexer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/lattice /lattice
COPY --from=build /out/lattice-indexer /lattice-indexer
EXPOSE 8080
ENTRYPOINT ["/lattice"]
