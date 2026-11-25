FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/app ./app && CGO_ENABLED=0 go build -o /out/trafficgen ./trafficgen

FROM gcr.io/distroless/static:nonroot AS app
COPY --from=build /out/app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]

FROM gcr.io/distroless/static:nonroot AS trafficgen
COPY --from=build /out/trafficgen /trafficgen
USER 65532:65532
ENTRYPOINT ["/trafficgen"]
