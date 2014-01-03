package kafka.clients.producer;

/**
 * An unserialized key/value pair to be sent to Kafka.
 */
public final class ProducerRecord {

    private final String topic;
    private final Object key;
    private final Object partitionKey;
    private final Object value;

    /**
     * Creates a record to be sent to Kafka using a special key for partitioning
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param partitionKey An override for the key to be used only for partitioning purposes in the client. This key
     *        will not be retained or available to downstream consumers.
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object key, Object partitionKey, Object value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.key = key;
        this.partitionKey = partitionKey;
        this.value = value;
    }

    /**
     * Create a record to be sent to Kafka
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object key, Object value) {
        this(topic, key, key, value);
    }

    /**
     * Create a record with no key
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object value) {
        this(topic, null, value);
    }

    /**
     * The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * The key (or null if no key is specified)
     */
    public Object key() {
        return key;
    }

    /**
     * @return The key to use for partitioning: the partitionKey if one is specified, otherwise the regular key
     */
    public Object partitionKey() {
        return partitionKey == null ? key : partitionKey;
    }

    /**
     * @return The value
     */
    public Object value() {
        return value;
    }

}
