package kabis.storage;

public class MessageWrapper<V> {
    private final V value;
    private final int senderId;

    public MessageWrapper(V value, int senderId) {
        this.value = value;
        this.senderId = senderId;
    }

    public MessageWrapper(V value){
        this(value, -1);
    }

    public V getValue() {
        return value;
    }

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
