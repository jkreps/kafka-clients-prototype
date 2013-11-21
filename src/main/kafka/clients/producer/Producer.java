package kafka.clients.producer;

public interface Producer {

	/**
	 * Send a message
	 */
	public SendResponse send(ProducerRecord record);
	
	/**
	 * Close this producer
	 */
	public void close();
}
