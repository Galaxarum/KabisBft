FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=safe-sense.jar
ENV SAFE_SENSE_HOME=/usr/kabis

WORKDIR $SAFE_SENSE_HOME
ADD $JAR_NAME    $SAFE_SENSE_HOME/$JAR_NAME
ADD safe-sense.properties $SAFE_SENSE_HOME/config.properties
COPY bft_config $SAFE_SENSE_HOME/config

ENTRYPOINT java -jar $JAR_NAME $CLIENT_ID $NUM_EXHIBITIONS $NUM_TRUE_ALARMS $NUM_FALSE_ALARMS