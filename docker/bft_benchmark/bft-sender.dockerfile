FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=bft-sender.jar
ENV KABIS_HOME=/usr/kabis

WORKDIR $KABIS_HOME
ADD $JAR_NAME    $KABIS_HOME/$JAR_NAME
COPY configs/bft_config $KABIS_HOME/config
ENTRYPOINT java -jar $JAR_NAME $ID $OPS_PER_SENDER $PAYLOAD_SIZE