FROM ghcr.io/maxisoft/nim-docker-images/nim as builder
# Build the executable
RUN mkdir -p '/usr/src/binancedumper'
COPY ./src/ /usr/src/binancedumper
WORKDIR /usr/src/binancedumper
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:/bin/binance_dumper "/usr/src/binancedumper/main.nim"


FROM alpine
LABEL maintainer="github.com/maxisoft" name="binance dumper" description="Track and save binance's futures data." url="https://github.com/maxisoft/binance-dumper" vcs-url="https://github.com/maxisoft/binance-dumper" org.opencontainers.image.source="https://github.com/maxisoft/binance-dumper"
RUN apk add --no-cache openssl binutils sqlite-dev ca-certificates pcre2 pcre
COPY --from=builder /bin/binance_dumper /bin/binance_dumper
ENTRYPOINT [ "/bin/binance_dumper" ]
#CMD [ ]