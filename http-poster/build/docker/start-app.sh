#!/bin/bash

[ -z "$JAVA_XMS" ] && JAVA_XMS=256m
[ -z "$JAVA_XMX" ] && JAVA_XMX=2048m

set -e
JAVA_OPTS="${JAVA_OPTS} \
-javaagent:${APP_HOME}/${JMXTRANS_AGENT}.jar=${APP_HOME}/jmxtrans-agent.xml \
-XX:+PrintGCDetails \
-XX:+PrintGCDateStamps \
-XX:+UseGCLogFileRotation \
-XX:NumberOfGCLogFiles=5 \
-XX:GCLogFileSize=2M \
-Xmx${JAVA_XMX} \
-Xms${JAVA_XMS} \
-Dapplication.name=${APP_NAME} \
-Dapplication.home=${APP_HOME}"

exec java ${JAVA_OPTS} -jar "${APP_HOME}/${APP_NAME}.jar"
