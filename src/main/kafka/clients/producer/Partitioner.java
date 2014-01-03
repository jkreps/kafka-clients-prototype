package kafka.clients.producer;

import kafka.common.Cluster;

/**
 * An interface by which clients can override the default partitioning behavior that maps records to topic partitions.
 */
public interface Partitioner {

    /**
     * Compute the partition for the given record
     * 
     * @param record The record being sent
     * @param cluster The current state of the cluster
     * @param numPartitions The total number of partitions for the given topic
     * @return The partition to send this record to
     */
    public int partition(ProducerRecord record, Cluster cluster, int numPartitions);

}
