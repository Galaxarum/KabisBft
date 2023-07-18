FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=ensure.jar
ENV ENSURE_HOME=/usr/kabis

WORKDIR $ENSURE_HOME
ADD $JAR_NAME    $ENSURE_HOME/$JAR_NAME
ADD ensure.properties $ENSURE_HOME/config.properties
COPY bft_config $ENSURE_HOME/config

ENTRYPOINT java -jar $JAR_NAME $CLIENT_ID $NUM_EXHIBITIONS $NUM_TRUE_ALARMS $NUM_FALSE_ALARMS $NUM_UNCAUGHT_BREACHES