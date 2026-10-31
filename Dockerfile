FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go test ./... \
    && CGO_ENABLED=0 go vet ./... \
    && CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /lore-mirror .

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /lore-mirror /lore-mirror
HEALTHCHECK --interval=30s --timeout=6s --start-period=30s --retries=3 CMD ["/lore-mirror", "-healthcheck"]
ENTRYPOINT ["/lore-mirror"]
