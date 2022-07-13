FROM golang:1.17-alpine AS builder

RUN apk --update add \
    util-linux-dev \
    build-base \
    musl-dev

ARG TARGETPLATFORM

ENV CGO_ENABLED=1
ENV GOPATH=/go

RUN export GOOS=$(echo ${TARGETPLATFORM} | cut -d / -f1) && \
    export GOARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2)

# Prepare and enter src directory
WORKDIR /go/src/github.com/signoz/migrate

# Cache dependencies
ADD go.mod .
ADD go.sum .
RUN go mod download -x

# Add the sources and proceed with build
ADD . .
RUN go build -o ./bin/migrate
RUN chmod +x ./bin/migrate


# use a minimal alpine image
FROM alpine:3.7

# Add Maintainer Info
LABEL maintainer="signoz"

# add ca-certificates in case you need them
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

# copy the binary from builder
COPY --from=builder /go/src/github.com/signoz/migrate/bin/migrate /usr/local/bin/migrate

# run the binary
ENTRYPOINT ["/usr/local/bin/migrate"]