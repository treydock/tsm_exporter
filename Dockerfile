ARG ARCH="amd64"
ARG OS="linux"
FROM ${ARCH}/centos:7
LABEL maintainer="Trey Dockendorf <treydock@gmail.com>"
ARG ARCH="amd64"
ARG OS="linux"
COPY .build/${OS}-${ARCH}/tsm_exporter /tsm_exporter
# Hack to ensure TSM 8.x share library name mismatch is accounted for
# This will have to be updated and added to as new versions of the shared library are released
RUN ln -s /opt/tivoli/tsm/client/api/bin64/libtsmxerces-c.so.28.0 /usr/lib64/libtsmxerces-c.so.28 && \
    ln -s /opt/tivoli/tsm/client/api/bin64/libtsmxerces-depdom.so.28.0 /usr/lib64/libtsmxerces-depdom.so.28
EXPOSE 9310
ENV PATH="/opt/tivoli/tsm/client/ba/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/ibm/gsk8_64/lib64:/opt/tivoli/tsm/client/api/bin64:/usr/lib64:"
ENTRYPOINT ["/tsm_exporter"]
