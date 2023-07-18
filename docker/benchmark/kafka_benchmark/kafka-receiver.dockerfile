FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=kafka-receiver.jar
ENV KABIS_HOME=/usr/kabis

WORKDIR $KABIS_HOME
ADD $JAR_NAME    $KABIS_HOME/$JAR_NAME
ADD consumer.properties $KABIS_HOME/config.properties
ENTRYPOINT java -jar $JAR_NAME $ID $OPS_PER_SENDER $NUM_SENDERS $PAYLOAD_SIZE