FROM golang:1.23-bookworm AS build-stage

LABEL org.opencontainers.image.source="https://github.com/thz/senstore"
LABEL org.opencontainers.image.description="thz/senstore scrape sensors and insert into db"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ADD . /go/src/github.com/thz/senstore
WORKDIR /go/src/github.com/thz/senstore

ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target="/root/.cache/go-build" env CGO_ENABLED=0 go build -o senstore ./cmd/senstore

FROM debian:bookworm-slim AS run-stage


# make the container slightly more useful for diagostics
RUN apt-get update && apt-get install -qq -y \
	inetutils-telnet \
	iputils-ping \
	openssl \
	socat

COPY --from=build-stage /go/src/github.com/thz/senstore/senstore /usr/bin/senstore

ENTRYPOINT [ "/usr/bin/senstore" ]
