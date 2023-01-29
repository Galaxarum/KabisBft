FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=kabis-sender.jar
ENV KABIS_HOME=/usr/kabis

WORKDIR $KABIS_HOME
ADD $JAR_NAME    $KABIS_HOME/$JAR_NAME
ADD producer.properties $KABIS_HOME/config.properties
COPY bft_config $KABIS_HOME/config
ENTRYPOINT java -jar $JAR_NAME $ID $NUM_OPERATIONS $PAYLOAD_SIZE $NUM_VALIDATED