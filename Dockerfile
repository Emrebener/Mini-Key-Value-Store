# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/minikv ./cmd/minikv

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/minikv /minikv

EXPOSE 11211

ENTRYPOINT ["/minikv"]
