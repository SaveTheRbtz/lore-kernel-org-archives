FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go test ./... \
    && CGO_ENABLED=0 go vet ./... \
    && CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o /lore-mirror ./cmd/lore-mirror

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /lore-mirror /lore-mirror
ENTRYPOINT ["/lore-mirror"]
