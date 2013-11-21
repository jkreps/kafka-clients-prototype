package kafka.clients.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import kafka.clients.common.network.Selector;
import kafka.clients.common.network.Send;
import kafka.clients.common.network.Receive;
import kafka.common.Cluster;
import kafka.common.TopicPartition;
import kafka.common.protocol.Protocol;
import kafka.common.protocol.types.Field;
import kafka.common.protocol.types.Record;
import kafka.clients.producer.RecordBuffers.RecordBuffer;

/**
 * Partition states: RESOLVING, CONNECTING, CONNECTED
 *
 */
public class Sender implements Runnable {
	
	private static enum PartitionState {RESOLVING, CONNECTING, CONNECTED}
	
	private final Map<TopicPartition, PartitionState> partitionState;
	private final RecordBuffers buffers;
	private final Selector selector;
	private final int maxBatchSize;
	private final long lingerMs;
	private Cluster cluster;
	private volatile boolean running;

	public Sender(Selector selector, 
			      RecordBuffers buffers, 
			      int maxBatchSize, 
			      long lingerMs) {
		this.partitionState = new HashMap<TopicPartition, PartitionState>();
		this.buffers = buffers;
		this.selector = selector;
		this.maxBatchSize = maxBatchSize;
		this.lingerMs = lingerMs;
		this.cluster = Cluster.empty();
		this.running = true;
	}
	
	/*
	 * partition info
	 *   - buffer for reuse
	 *   - 
	 */
	public void run() {
		Requests requests = new Requests();
		while(running) {
			// ready partitions - in flight or blacklisted partitions
			long now = System.currentTimeMillis();
			List<TopicPartition> ready = this.buffers.ready(now);
			// handle new connections
			List<RecordBuffer> buffers = this.buffers.drain(ready);
			
			collate(cluster, buffers, requests.sends);
			//this.selector.poll(1000L, requests.sends, requests.completed, requests.receives, requests.disconnects);
			
			// handle completed sends
			
			// handle completed requests
			
		}
	}
	
	void collate(Cluster cluster, List<RecordBuffer> buffers, List<Send> sends) {
		for(RecordBuffer buffer: buffers) {
		
		}
	}
	
	void transmit(Requests requests) {
		
	}
	
	void wakeup() {
		this.selector.wakeup();
	}
	
	static class Requests {
		public final List<Send> sends = new ArrayList<Send>();
		public final List<Send> completed = new ArrayList<Send>();
		public List<Receive> receives = new ArrayList<Receive>();
		public List<Integer> disconnects = new ArrayList<Integer>();
	}
	
}
