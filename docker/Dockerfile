FROM ghcr.io/maxisoft/nim-docker-images/nim as builder
# Build the executable
RUN mkdir -p '/usr/src/binancedumper' '/binancedumper'
COPY ./src/ /usr/src/binancedumper
WORKDIR /usr/src/binancedumper
RUN nimble install -y ws asynctools sorta
RUN nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:/bin/binance_dumper "/usr/src/binancedumper/main.nim"


FROM ghcr.io/maxisoft/nim-docker-images/nim-runtime
ARG uid=1001 # the User id used to run program / create new output files
ARG gid=1001
LABEL maintainer="github.com/maxisoft" name="binance dumper" description="Track and save binance's futures data." url="https://github.com/maxisoft/binance-dumper" vcs-url="https://github.com/maxisoft/binance-dumper" org.opencontainers.image.source="https://github.com/maxisoft/binance-dumper"
RUN \
    addgroup --system --gid $gid binancedumper && \
    adduser --system --uid $uid --ingroup binancedumper --shell /bin/sh binancedumper && \
    mkdir -p "/binancedumper" && \
    chown -R binancedumper:binancedumper "/binancedumper"
COPY --from=builder --chown=binancedumper:binancedumper /bin/binance_dumper /bin/binance_dumper
VOLUME [ "/binancedumper" ]
WORKDIR /binancedumper
USER binancedumper
ENTRYPOINT [ "/bin/binance_dumper" ]
#CMD [ ]