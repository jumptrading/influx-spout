##Build influx-spout
FROM golang:1 as go-builder
WORKDIR /go/src/app
RUN ["bash", "-c", "git clone --depth=1 https://github.com/jumptrading/influx-spout.git ."]
RUN ["bash", "-c", "make deps influx-spout influx-spout-tap"]

##Use binaries and config in alpine production container
FROM alpine:3
COPY --from=go-builder /go/src/app/influx-spout /bin/
COPY --from=go-builder /go/src/app/influx-spout-tap /bin/
COPY --from=go-builder /go/src/app/example-udp-listener.conf /etc/influx-spout/udp-listener.toml
COPY --from=go-builder /go/src/app/example-http-listener.conf /etc/influx-spout/http-listener.toml
RUN apk update && apk add dumb-init && rm -rf /var/cache/apk/*

# Allow influx-spout to receive signals
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/bin/influx-spout"]
CMD ["/etc/influx-spout/http-listener.toml"]

