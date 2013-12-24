package kafka.clients.producer;

/**
 * A record to be sent to Kafka
 */
public final class ProducerRecord {
	
	private final String topic;
	private final Object key;
	private final Object partitionKey;
	private final Object value;
	
	/**
	 * Creates a record to be sent to Kafka using a special key for partitioning
	 * @param topic The topic the record will be appended to
	 * @param key The key for the record
	 * @param partitionKey An override for the key to be used only for partitioning purposes
	 * @param value The value of the record
	 */
	public ProducerRecord(String topic, 
			                  Object key, 
			                  Object partitionKey,
			                  Object value) {
	  if(topic == null)
	    throw new IllegalArgumentException("Topic cannot be null");
		this.topic = topic;
		this.key = key;
		this.partitionKey = partitionKey;
		this.value = value;
	}
	
	/**
	 * Create a record to be sent to Kafka
	 * @param topic The topic the record will be appended to
	 * @param key The key for the record
	 * @param value The value for the record
	 */
	public ProducerRecord(String topic, Object key, Object value) {
		this(topic, key, key, value);
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
	 * @return The key to use for partitioning: the partitionKey if one is specified, otherwise the key
	 */
	public Object partitionKey() {
		return partitionKey == null? key : partitionKey;
	}

	/**
	 * @return The value
	 */
	public Object value() {
		return value;
	}
	
}
