FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /bin/marketdata ./cmd

FROM debian:bookworm-slim
COPY --from=build /bin/marketdata /usr/local/bin/marketdata
EXPOSE 8085
ENTRYPOINT ["marketdata"]
