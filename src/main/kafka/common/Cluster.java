package kafka.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cluster {
	private final Map<Integer, Node> nodes;
	private final Map<TopicPartition, PartitionInfo> partitions;
	
	public Cluster(List<Node> nodes, List<PartitionInfo> partitions) {
		this.nodes = new HashMap<Integer, Node>(nodes.size());
		this.partitions = new HashMap<TopicPartition, PartitionInfo>(partitions.size());
		for(Node n: nodes)
			this.nodes.put(n.id(), n);
		for(PartitionInfo p: partitions)
			this.partitions.put(new TopicPartition(p.topic(), p.partition()), p);
	}
	
	public static Cluster empty() {
		return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0));
	}
	
	public Node leaderFor(TopicPartition tp) {
		PartitionInfo info = partitions.get(tp);
		if(info == null)
			return null;
		else 
			return nodes.get(info.leader());
	}
}
