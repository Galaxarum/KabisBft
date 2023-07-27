package kabis.art_exhibition.kafka_only;


public enum Topics {
    ART_EXHIBITION("ART-EXHIBITION");

    private final String topic;

    Topics(final String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return topic;
    }
}
