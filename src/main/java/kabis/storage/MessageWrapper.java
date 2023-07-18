package kabis.storage;

public class MessageWrapper<V> {
    private final V value;
    private final int senderId;

    /**
     * Creates a new MessageWrapper.
     *
     * @param value    the value of the message
     * @param senderId the id of the sender
     */
    public MessageWrapper(V value, int senderId) {
        this.value = value;
        this.senderId = senderId;
    }

    /**
     * Creates a new MessageWrapper.
     * <p>
     * The senderId is set to -1.
     *
     * @param value the value of the message
     */
    public MessageWrapper(V value) {
        this(value, -1);
    }

    /**
     * Returns the value of the message.
     *
     * @return the value of the message
     */
    public V getValue() {
        return value;
    }

    /**
     * Returns the id of the sender.
     *
     * @return the id of the sender
     */
    public int getSenderId() {
        return senderId;
    }

    @Override
    public String toString() {
        return "MessageWrapper{" +
                "value=" + value +
                ", senderId=" + senderId +
                '}';
    }
}
