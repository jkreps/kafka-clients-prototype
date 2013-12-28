package kafka.clients.producer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.record.MemoryRecords;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.Time;
import kafka.common.utils.Utils;

/**
 * This class accumulates records that are waiting to be sent in per-partition buffers.
 */
public final class RecordAccumulator {
	
  private int drainIndex = 0;
	private final int batchSize;
	private final long lingerMs;
	private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
	private final BufferPool free;
	private final Time time;
	
	public RecordAccumulator(int batchSize, int totalSize, long lingerMs, Time time) {
		this.batchSize = batchSize;
		this.lingerMs = lingerMs;
		this.batches = new ConcurrentHashMap<TopicPartition, Deque<RecordBatch>>();
		this.free = new BufferPool(totalSize, batchSize);
		this.time = time;
	}
	
	/**
	 * Add a record to the accumulator
	 * @param tp The topic-partition to add the record to
	 * @param key The record's key
	 * @param value The record's value
	 * @param callback The callback to be executed on completion of the request (or null if none)
	 * @return The pending record send object
	 */
	public RecordSend append(TopicPartition tp, byte[] key, byte[] value, CompressionType compression, Callback callback) throws InterruptedException {
		// check if we have an in-progress batch
		Deque<RecordBatch> dq = dequeFor(tp);
		synchronized(dq) {
			RecordBatch batch = dq.peekLast();
			if(batch != null) {
				RecordSend send = batch.tryAppend(key, value, compression, callback);
				if(send != null)
				  return send;
			}
		}
		
    // we don't have an in-progress record batch try to allocate a new batch
    int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key.length, value.length));
    ByteBuffer buffer = free.allocate(size);
    synchronized(dq) {
  	  RecordBatch first = dq.peekLast();
      if(first != null) {
  			RecordSend send = first.tryAppend(key, value, compression, callback);
  			if(send != null) {
    		  // somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
    		  free.deallocate(buffer);
  	  	  return send;
  			}
  	  }
      RecordBatch batch = new RecordBatch(tp, new MemoryRecords(buffer), time.milliseconds());
      RecordSend send = Utils.notNull(batch.tryAppend(key, value, compression, callback));
  	  dq.addLast(batch);
  	  return send;
		}
	}
	
	/**
	 * Get a list of topic-partitions which are ready to be sent
	 * @param now The current time
	 * @return A list of topic-partitions
	 */
	public List<TopicPartition> ready(long now) {
		List<TopicPartition> ready = new ArrayList<TopicPartition>();
		for(Map.Entry<TopicPartition, Deque<RecordBatch>> entry: this.batches.entrySet()) {
			Deque<RecordBatch> deque = entry.getValue();
			synchronized(deque) {
				RecordBatch batch = deque.peekFirst();
				boolean full = deque.size() > 1;
				boolean expired = batch != null && now - batch.created >= lingerMs;
				if(full | expired)
					ready.add(batch.topicPartition);
			}
		}
		return ready;
	}
	
	/**
	 * Drain all the data for the given topic-partitions
	 * @param partitions The topic-partitions to drain
	 * @return The list of batches
	 * TODO: There may be a starvation issue due to iteration order
	 */
	public List<RecordBatch> drain(List<TopicPartition> partitions, int maxSize) {
	  int size = 0;
		List<RecordBatch> ready = new ArrayList<RecordBatch>();
		/* to make starvation less likely this loop doesn't start at 0 */
		int start = drainIndex = drainIndex % partitions.size();
		do {
		  TopicPartition tp = partitions.get(drainIndex);
			Deque<RecordBatch> deque = this.batches.get(tp);
			if(deque != null) {
				synchronized(deque) {
					if(size + deque.peekFirst().records.sizeInBytes() > maxSize)
					  return ready;
					else
  					ready.add(deque.pollFirst());
				}
			}
			this.drainIndex = (this.drainIndex + 1) % partitions.size();
		} while(start != drainIndex);
		return ready;
	}
	
	/**
	 * Get the deque for the given topic-partition, creating it if necessary
	 * @param tp The topic partition
	 * @return The deque
	 */
	private Deque<RecordBatch> dequeFor(TopicPartition tp) {
		Deque<RecordBatch> d = this.batches.get(tp);
		if(d == null) {
			this.batches.putIfAbsent(tp, new ArrayDeque<RecordBatch>());
			d = this.batches.get(tp);
		}
		return d;
	}
	
	/**
	 * Deallocate the list of record batches
	 * @param batches The batches to deallocate
	 */
	public void deallocate(List<RecordBatch> batches) {
	  ByteBuffer[] buffers = new ByteBuffer[batches.size()];
	  for(int i = 0; i < batches.size(); i++)
	    buffers[i] = batches.get(i).records.buffer();
	  free.deallocate(buffers);
	}
	
}
