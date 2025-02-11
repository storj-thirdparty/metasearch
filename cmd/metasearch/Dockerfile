ARG DOCKER_ARCH

FROM ${DOCKER_ARCH:-amd64}/debian:buster-slim
ARG TAG
ARG GOARCH
ENV GOARCH=${GOARCH}
ENV CONF_PATH=/root/.local/share/storj/metasearch 
ENV PATH=$PATH:/app
EXPOSE 6666
WORKDIR /app

COPY cmd/metasearch/migration /app/migration
COPY cmd/metasearch/metasearch /app/metasearch
COPY cmd/metasearch/entrypoint /entrypoint
 

ENTRYPOINT ["/entrypoint"]