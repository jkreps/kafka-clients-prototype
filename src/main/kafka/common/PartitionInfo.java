package kafka.common;

public class PartitionInfo {
	
	private final String topic;
	private final int partition;
	private final int leader;
	private final int[] replicas;
	private final int[] inSyncReplicas;
	
	public PartitionInfo(String topic, int partition, int leader, int[] replicas, int[] inSyncReplicas) {
		this.topic = topic;
		this.partition = partition;
		this.leader = leader;
		this.replicas = replicas;
		this.inSyncReplicas = inSyncReplicas;
	}

	public String topic() {
		return topic;
	}

	public int partition() {
		return partition;
	}

	public int leader() {
		return leader;
	}

	public int[] replicas() {
		return replicas;
	}

	public int[] inSyncReplicas() {
		return inSyncReplicas;
	}

}
