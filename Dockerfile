FROM alpine:3.7
ADD influx-spout /bin/
ADD influx-spout-tap /bin/
ADD example-udp-listener.conf /etc/influx-spout/udp-listener.toml
ADD example-http-listener.conf /etc/influx-spout/http-listener.toml
RUN apk update && apk add dumb-init && rm -rf /var/cache/apk/*

# Allow influx-spout to receive signals
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/bin/influx-spout"]
CMD ["/etc/influx-spout/http-listener.toml"]
