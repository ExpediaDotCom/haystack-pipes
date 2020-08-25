FROM haystack-pipes-kafka-producer:latest
MAINTAINER Haystack <haystack@expedia.com>


RUN mkdir -p ${APP_HOME}/extractors/
COPY target/haystack-sample-key-extractors.jar ${APP_HOME}/extractors/
#COPY build/docker/extractors.conf ${APP_HOME}/
#ENV EXTRACTOR_CONFIG_PATH ${APP_HOME}/extractors.conf
RUN ls ${APP_HOME}/extractors/




WORKDIR ${APP_HOME}
