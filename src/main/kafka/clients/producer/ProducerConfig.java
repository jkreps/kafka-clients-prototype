package kafka.clients.producer;

import static kafka.common.config.ConfigDef.Range.atLeast;
import static kafka.common.config.ConfigDef.Range.between;

import java.util.Map;

import kafka.common.config.AbstractConfig;
import kafka.common.config.ConfigDef;
import kafka.common.config.ConfigDef.Type;

/**
 * The producer configuration keys
 */
public class ProducerConfig extends AbstractConfig {

    private static final ConfigDef config;

    public static final String BROKER_LIST_CONFIG = "metadata.broker.list";
    public static final String METADATA_FETCH_TIMEOUT_CONFIG = "metadata.fetch.timeout.ms";
    public static final String MAX_PARTITION_SIZE_CONFIG = "max.partition.bytes";
    public static final String TOTAL_BUFFER_MEMORY_CONFIG = "total.memory.bytes";
    public static final String REQUIRED_ACKS_CONFIG = "request.required.acks";
    public static final String REQUEST_TIMEOUT_CONFIG = "request.timeout.ms";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer.class";
    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer.class";
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    public static final String METADATA_REFRESH_MS_CONFIG = "topic.metadata.refresh.interval.ms";
    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
    public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    public static final String BLOCK_ON_BUFFER_FULL = "block.on.buffer.full";

    static {
        /* TODO: add docs */
        config = new ConfigDef().define(BROKER_LIST_CONFIG, Type.LIST, "blah blah")
                                .define(METADATA_FETCH_TIMEOUT_CONFIG, Type.LONG, 60 * 1000, atLeast(0), "blah blah")
                                .define(MAX_PARTITION_SIZE_CONFIG, Type.INT, 16384, atLeast(0), "blah blah")
                                .define(TOTAL_BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), "blah blah")
                                /* TODO: should be a string to handle acks=in-sync */
                                .define(REQUIRED_ACKS_CONFIG, Type.INT, 1, between(-1, Short.MAX_VALUE), "blah blah")
                                .define(REQUEST_TIMEOUT_CONFIG, Type.INT, 30 * 1000, atLeast(0), "blah blah")
                                .define(LINGER_MS_CONFIG, Type.LONG, 0, atLeast(0L), "blah blah")
                                .define(VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS, "blah blah")
                                .define(KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, "blah blah")
                                .define(PARTITIONER_CLASS_CONFIG, Type.CLASS, DefaultPartitioner.class.getName(), "blah blah")
                                .define(METADATA_REFRESH_MS_CONFIG, Type.LONG, 10 * 60 * 1000, atLeast(-1L), "blah blah")
                                .define(CLIENT_ID_CONFIG, Type.STRING, "", "blah blah")
                                .define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(0), "blah blah")
                                .define(MAX_REQUEST_SIZE_CONFIG, Type.INT, 1 * 1024 * 1024, atLeast(0), "blah blah")
                                .define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 10L, atLeast(0L), "blah blah")
                                .define(BLOCK_ON_BUFFER_FULL, Type.BOOLEAN, true, "blah blah");
    }

    ProducerConfig(Map<? extends Object, ? extends Object> props) {
        super(config, props);
    }

}
