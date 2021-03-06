package kafka.clients.producer;

/**
 * The interface for the {@link KafkaProducer}
 * 
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer {

    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * 
     * @param record The record to send
     * @return A future which will eventually contain the response information
     */
    public RecordSend send(ProducerRecord record);

    /**
     * Send a message and invoke the given callback when the send is complete
     */
    public RecordSend send(ProducerRecord record, Callback callback);

    /**
     * Close this producer
     */
    public void close();

}
