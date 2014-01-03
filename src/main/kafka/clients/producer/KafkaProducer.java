package kafka.clients.producer;

import static kafka.clients.producer.ProducerConfig.BLOCK_ON_BUFFER_FULL;
import static kafka.clients.producer.ProducerConfig.BROKER_LIST_CONFIG;
import static kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static kafka.clients.producer.ProducerConfig.MAX_PARTITION_SIZE_CONFIG;
import static kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_CONFIG;
import static kafka.clients.producer.ProducerConfig.REQUIRED_ACKS_CONFIG;
import static kafka.clients.producer.ProducerConfig.TOTAL_BUFFER_MEMORY_CONFIG;
import static kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.clients.common.network.Selector;
import kafka.common.Cluster;
import kafka.common.KafkaException;
import kafka.common.Serializer;
import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.utils.KafkaThread;
import kafka.common.utils.SystemTime;

/**
 * A Kafka producer that can be used to send data to the Kafka cluster. The producer is thread safe and should be shared
 * among all threads for best performance.
 */
public class KafkaProducer implements Producer {

    private final Partitioner partitioner;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final Thread ioThread;

    /**
     * The producer configuration. Valid configuration keys are documented <a
     * href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be either strings or
     * Objects of the appropriate type (String, Integer, Long, Double, List, Class).
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs));
    }

    /**
     * The producer configuration. Valid configuration keys are documented <a
     * href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties));
    }

    private KafkaProducer(ProducerConfig config) {
        this.keySerializer = config.getConfiguredInstance(KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.valueSerializer = config.getConfiguredInstance(VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.partitioner = config.getConfiguredInstance(PARTITIONER_CLASS_CONFIG, Partitioner.class);
        this.metadata = new Metadata();
        this.accumulator = new RecordAccumulator(config.getInt(MAX_PARTITION_SIZE_CONFIG),
                                                 config.getLong(TOTAL_BUFFER_MEMORY_CONFIG),
                                                 config.getLong(LINGER_MS_CONFIG),
                                                 config.getBoolean(BLOCK_ON_BUFFER_FULL),
                                                 new SystemTime());
        List<InetSocketAddress> urls = parseAddresses(config.getList(BROKER_LIST_CONFIG));
        this.metadata.update(Cluster.bootstrap(urls), System.currentTimeMillis());
        this.sender = new Sender(new Selector(),
                                 this.metadata,
                                 this.accumulator,
                                 config.getString(CLIENT_ID_CONFIG),
                                 config.getInt(MAX_REQUEST_SIZE_CONFIG),
                                 config.getLong(RECONNECT_BACKOFF_MS_CONFIG),
                                 (short) config.getInt(REQUIRED_ACKS_CONFIG),
                                 config.getInt(REQUEST_TIMEOUT_CONFIG),
                                 new SystemTime());
        this.ioThread = new KafkaThread("kafka-network-thread", this.sender, false);
        this.ioThread.start();
    }

    private static List<InetSocketAddress> parseAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String url : urls) {
            if (url != null && url.length() > 0) {
                String[] pieces = url.split(":");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid url in metadata.broker.list: " + url);
                try {
                    // TODO: This actually does DNS resolution (?), which is a little weird
                    addresses.add(new InetSocketAddress(pieces[0], Integer.parseInt(pieces[1])));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port in metadata.broker.list: " + url);
                }
            }
        }
        if (addresses.size() < 1)
            throw new IllegalArgumentException("No bootstrap urls given in metadata.broker.list.");
        return addresses;
    }

    /**
     * Equivalent to send(record, null)
     */
    @Override
    public RecordSend send(ProducerRecord record) {
        return send(record, null);
    }

    /**
     * Send a record.
     * 
     * The send is asynchronous and this method will return immediately.
     * 
     * The RecordSend returned by this call will hold the future response when it is ready. This object can be used to
     * make the call synchronous:
     * 
     * <pre>
     *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
     *   producer.send(myRecord, null).await();
     * </pre>
     * 
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server
     * 
     */
    @Override
    public RecordSend send(ProducerRecord record,
                           Callback callback) {
        Cluster cluster = metadata.fetch(record.topic());
        int partition = partitioner.partition(record, cluster, cluster.partitionsFor(record.topic()).size());
        byte[] key = keySerializer.toBytes(record.key());
        byte[] value = valueSerializer.toBytes(record.value());
        try {
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            RecordSend send = accumulator.append(tp, key, value, CompressionType.NONE, callback);
            this.sender.wakeup();
            return send;
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Close this producer. This method blocks until all in-flight requests complete.
     */
    @Override
    public void close() {
        this.sender.initiateClose();
        try {
            this.ioThread.join();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

}
