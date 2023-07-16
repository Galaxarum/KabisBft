FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=safe-corp.jar
ENV SAFE_CORP_HOME=/usr/kabis

WORKDIR $SAFE_CORP_HOME
ADD $JAR_NAME    $SAFE_CORP_HOME/$JAR_NAME
ADD safe-corp.properties $SAFE_CORP_HOME/consumer.config.properties
ADD producer.properties $SAFE_SENSE_HOME/producer.config.properties
COPY bft_config $SAFE_CORP_HOME/config

ENTRYPOINT java -jar $JAR_NAME $CLIENT_ID $NUM_EXHIBITIONS $NUM_TRUE_ALARMS $NUM_FALSE_ALARMS $NUM_UNCAUGHT_BREACHES