FROM golang:1.17-alpine AS builder

ARG TARGETPLATFORM

ENV CGO_ENABLED=1
ENV GOPATH=/go

RUN export GOOS=$(echo ${TARGETPLATFORM} | cut -d / -f1) && \
    export GOARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2)

RUN apk --update add \
    util-linux-dev \
    build-base \
    musl-dev

# Prepare and enter src directory
WORKDIR /go/src/github.com/signoz/migrate-dashboard

# Cache dependencies
ADD go.mod .
ADD go.sum .
RUN go mod download -x

# Add the sources and proceed with build
ADD . .
RUN go build -o migrate-dashboard  -a -ldflags "-linkmode external -extldflags '-static' -s -w" main.go
RUN chmod +x migrate-dashboard


# use scratch
FROM scratch

# Add Maintainer Info
LABEL maintainer="signoz"

# copy the binary from builder
COPY --from=builder /go/src/github.com/signoz/migrate-dashboard/migrate-dashboard /usr/local/bin/migrate-dashboard

# run the binary
ENTRYPOINT ["/usr/local/bin/migrate-dashboard"]

CMD ["--dataSource", "/var/lib/signoz/signoz.db"]
