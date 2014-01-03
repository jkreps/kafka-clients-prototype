package kafka.clients.producer;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.Cluster;
import kafka.common.utils.Utils;

/**
 * A simple partitioner that computes the partition using the murmur2 hash of the serialized key. This partitioner will
 * use the partitionKey if it is supplied, otherwise it will partition based on the record key. When neither key is
 * provided this partitioner will round-robin over all available partitions.
 */
public class DefaultPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(ProducerRecord record, byte[] key, byte[] partitionKey, byte[] value, Cluster cluster, int numPartitions) {
        byte[] keyToUse = partitionKey != null ? partitionKey : key;
        if (keyToUse == null)
            return Utils.abs(counter.getAndIncrement()) % numPartitions;
        else
            return Utils.abs(Utils.murmur2(keyToUse)) % numPartitions;
    }

}
