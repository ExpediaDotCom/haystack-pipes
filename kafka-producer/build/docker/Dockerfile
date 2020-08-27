FROM openjdk:8-jre
MAINTAINER Haystack <haystack@expedia.com>

ENV APP_NAME haystack-pipes-kafka-producer
ENV APP_HOME /app/bin
ENV JMXTRANS_AGENT jmxtrans-agent-1.2.6
ENV EXTRACTOR_DIRECTORY extractors/


RUN mkdir -p ${APP_HOME}
RUN mkdir -p ${APP_HOME}/${EXTRACTOR_DIRECTORY}

COPY target/${APP_NAME}.jar ${APP_HOME}/
COPY build/docker/start-app.sh ${APP_HOME}/
COPY build/docker/jmxtrans-agent.xml ${APP_HOME}/
COPY ../sample-key-extractor/target/sample-key-extractor.jar ${APP_HOME}/${EXTRACTOR_DIRECTORY}

RUN chmod +x ${APP_HOME}/start-app.sh
ADD https://github.com/jmxtrans/jmxtrans-agent/releases/download/${JMXTRANS_AGENT}/${JMXTRANS_AGENT}.jar ${APP_HOME}/

WORKDIR ${APP_HOME}

ENTRYPOINT ["./start-app.sh"]
