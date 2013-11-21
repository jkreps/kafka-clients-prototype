package kafka.clients.producer;

public class ProducerRecord {
	
	private final String topic;
	private final Object key;
	private final Object partitionKey;
	private final Object value;
	
	public ProducerRecord(String topic, 
			              Object key, 
			              Object partitionKey,
			              Object value) {
		this.topic = topic;
		this.key = key;
		this.partitionKey = partitionKey;
		this.value = value;
	}
	
	public ProducerRecord(String topic, Object key, Object value) {
		this(topic, key, key, value);
	}

	public String topic() {
		return topic;
	}

	public Object key() {
		return key;
	}

	public Object partitionKey() {
		return partitionKey;
	}

	public Object value() {
		return value;
	}
	
}
