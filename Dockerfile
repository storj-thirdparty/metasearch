ARG BUILDPLATFORM

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.23 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/metasearch cmd/metasearch/main.go

FROM build-stage AS run-test-stage
RUN go test -v ./...

FROM busybox:1.35.0-uclibc as busybox

FROM --platform=${BUILDPLATFORM:-linux/amd64} gcr.io/distroless/base-debian11 AS build-release-stage

WORKDIR /app

ENV CONF_PATH=/root/.local/share/storj/metasearch 
ENV PATH=$PATH:/app
EXPOSE 6666

COPY --from=busybox /bin/sh /bin/sh
COPY --from=build-stage /app/metasearch /app/metasearch
COPY cmd/metasearch/entrypoint /entrypoint

ENTRYPOINT ["/entrypoint"]
