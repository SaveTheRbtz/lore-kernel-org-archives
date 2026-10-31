FROM golang:1.27 AS build
ENV CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go test ./... \
    && go vet ./... \
    && go build -buildvcs=false -trimpath -ldflags='-s -w' -o /lore-mirror ./cmd/lore-mirror

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /lore-mirror /lore-mirror
ENTRYPOINT ["/lore-mirror"]
