package kafka.clients.producer;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.Cluster;
import kafka.common.utils.Utils;

/**
 * A simple partitioner that computes the partition using the hash code of the key
 */
public class DefaultPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(ProducerRecord record,
                         Cluster cluster,
                         int numPartitions) {
        Object key = record.partitionKey();
        if (key == null)
            return Utils.abs(counter.getAndIncrement()) % numPartitions;
        else
            return Utils.abs(key.hashCode()) % numPartitions;
    }

}
