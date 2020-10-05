FROM haystack-pipes-kafka-producer:latest
MAINTAINER Haystack <haystack@expedia.com>

RUN mkdir -p ${APP_HOME}/extractors/
COPY target/sample-key-extractor.jar ${APP_HOME}/extractors/
RUN ls ${APP_HOME}/extractors/

WORKDIR ${APP_HOME}
