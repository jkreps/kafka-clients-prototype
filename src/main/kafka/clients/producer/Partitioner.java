package kafka.clients.producer;

import kafka.common.Cluster;

public interface Partitioner {

	public int partition(Object key, Object value, Cluster cluster);
	
}
