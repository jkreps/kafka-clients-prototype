package kafka.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.utils.Utils;

public final class Cluster {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Node> nodes;
    private final Map<Integer, Node> nodesById;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;

    public Cluster(List<Node> nodes, List<PartitionInfo> partitions) {
        this.nodes = new ArrayList<Node>(nodes);
        this.nodesById = new HashMap<Integer, Node>(nodes.size());
        this.partitionsByTopicPartition = new HashMap<TopicPartition, PartitionInfo>(partitions.size());
        this.partitionsByTopic = new HashMap<String, List<PartitionInfo>>(partitions.size());

        Collections.shuffle(nodes);
        for (Node n : nodes)
            this.nodesById.put(n.id(), n);
        for (PartitionInfo p : partitions)
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        for (PartitionInfo p : partitions) {
            if (!this.partitionsByTopic.containsKey(p.topic()))
                this.partitionsByTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> ps = this.partitionsByTopic.get(p.topic());
            ps.add(p);
        }
    }

    public static Cluster empty() {
        return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0));
    }

    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<Node>();
        int nodeId = Integer.MIN_VALUE;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId++, address.getHostName(), address.getPort()));
        return new Cluster(nodes, new ArrayList<PartitionInfo>(0));
    }

    public Node leaderFor(TopicPartition tp) {
        PartitionInfo info = partitionsByTopicPartition.get(tp);
        if (info == null)
            return null;
        else
            return nodesById.get(info.leader());
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.partitionsByTopic.get(topic);
    }

    public Node nextNode() {
        int size = nodes.size();
        if (size == 0)
            throw new IllegalStateException("No known nodes.");
        int idx = Utils.abs(counter.getAndIncrement()) % size;
        return this.nodes.get(idx);
    }

}
