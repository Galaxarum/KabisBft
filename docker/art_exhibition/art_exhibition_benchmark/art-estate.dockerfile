FROM adoptopenjdk/openjdk11:alpine-jre
ENV JAR_NAME=art-estate.jar
ENV ART_ESTATE_HOME=/usr/kabis

WORKDIR $ART_ESTATE_HOME
ADD $JAR_NAME    $ART_ESTATE_HOME/$JAR_NAME
ADD consumer.properties $ART_ESTATE_HOME/config.properties
COPY bft_config $ART_ESTATE_HOME/config

ENTRYPOINT java -jar $JAR_NAME $NUM_EXHIBITIONS $NUM_TRUE_ALARMS $NUM_FALSE_ALARMS $NUM_UNCAUGHT_BREACHES