##Build influx-spout
FROM golang:1 as go-builder
WORKDIR /go/src/github.com/jumptrading/influx-spout

#Obtaining source (choose 1)
RUN ["bash", "-c", "git clone --depth=1 https://github.com/jumptrading/influx-spout.git ."]
#Alternatively, you can change to use local source (example is this directory), with;
#COPY ./ ./

#Build
RUN ["bash", "-c", "make influx-spout influx-spout-tap"]

##Use binaries and config in alpine production container
FROM alpine:3
COPY --from=go-builder /go/src/github.com/jumptrading/influx-spout/influx-spout /bin/
COPY --from=go-builder /go/src/github.com/jumptrading/influx-spout/influx-spout-tap /bin/
COPY --from=go-builder /go/src/github.com/jumptrading/influx-spout/example-udp-listener.conf /etc/influx-spout/udp-listener.toml
COPY --from=go-builder /go/src/github.com/jumptrading/influx-spout/example-http-listener.conf /etc/influx-spout/http-listener.toml
RUN apk update && apk add dumb-init && rm -rf /var/cache/apk/*

# Allow influx-spout to receive signals
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/bin/influx-spout"]
CMD ["/etc/influx-spout/http-listener.toml"]

