package kafka.clients.producer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.record.MemoryRecords;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.Time;
import kafka.common.utils.Utils;

/**
 * This class accumulates records that are waiting to be sent in per-partition buffers.
 * TODO: Current behavior is to send all when memory is exhausted
 */
public final class RecordAccumulator {
	
  private volatile boolean closed;
  private int drainIndex;
	private final int batchSize;
	private final long lingerMs;
	private final AtomicReference<Map<TopicPartition, Deque<RecordBatch>>> batches;
	private final BufferPool free;
	private final Time time;
	
	public RecordAccumulator(int batchSize, long totalSize, long lingerMs, Time time) {
	  this.drainIndex = 0;
	  this.closed = false;
		this.batchSize = batchSize;
		this.lingerMs = lingerMs;
		this.batches = new AtomicReference<Map<TopicPartition, Deque<RecordBatch>>>(new HashMap<TopicPartition, Deque<RecordBatch>>());
		this.free = new BufferPool(totalSize, batchSize);
		this.time = time;
	}
	
	/**
	 * Add a record to the accumulator
	 */
	public RecordSend append(TopicPartition tp, byte[] key, byte[] value, CompressionType compression, Callback callback) throws InterruptedException {
	  if(closed)
	    throw new IllegalStateException("Cannot send after the producer is closed.");
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
    int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
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
	 */
	public List<TopicPartition> ready(long now) {
		List<TopicPartition> ready = new ArrayList<TopicPartition>();
    boolean exhausted = this.free.queued() > 0;
		for(Map.Entry<TopicPartition, Deque<RecordBatch>> entry: this.batches.get().entrySet()) {
			Deque<RecordBatch> deque = entry.getValue();
			synchronized(deque) {
				RecordBatch batch = deque.peekFirst();
				if(batch != null) {
				  boolean full = deque.size() > 1;
				  boolean expired = now - batch.created >= lingerMs;
				  if(full | expired | exhausted | closed)
					  ready.add(batch.topicPartition);
				}
			}
		}
		return ready;
	}
	
	/**
	 * Drain all the data for the given topic-partitions
	 * TODO: There may be a starvation issue due to iteration order
	 */
	public List<RecordBatch> drain(List<TopicPartition> partitions, int maxSize) {
	  if(partitions.isEmpty())
	    return Collections.emptyList();
	  int size = 0;
		List<RecordBatch> ready = new ArrayList<RecordBatch>();
		/* to make starvation less likely this loop doesn't start at 0 */
		int start = drainIndex = drainIndex % partitions.size();
		do {
		  TopicPartition tp = partitions.get(drainIndex);
			Deque<RecordBatch> deque = dequeFor(tp);
			if(deque != null) {
				synchronized(deque) {
					if(size + deque.peekFirst().records.sizeInBytes() > maxSize) {
					  return ready;
					} else {
					  RecordBatch batch = deque.pollFirst();
					  size += batch.records.sizeInBytes();
  					ready.add(batch);
					}
				}
			}
			this.drainIndex = (this.drainIndex + 1) % partitions.size();
		} while(start != drainIndex);
		return ready;
	}
	
	/**
	 * Get the deque for the given topic-partition, creating it if necessary
	 */
  private Deque<RecordBatch> dequeFor(TopicPartition tp) {
    Deque<RecordBatch> d = this.batches.get().get(tp);
    if(d != null)
      return d;
    
    // copy-on-write the map
    Deque<RecordBatch> deque = new ArrayDeque<RecordBatch>();
    while(true) {
      Map<TopicPartition, Deque<RecordBatch>> map = this.batches.get();
      Map<TopicPartition, Deque<RecordBatch>> copy = new HashMap<TopicPartition, Deque<RecordBatch>>(map);
      Deque<RecordBatch> prev = copy.put(tp, deque);
      // if there was already something there then the just use that (another thread got there)
      if(prev != null)
        return prev; 
      // only update the reference if someone else hasn't already
      boolean updated = this.batches.compareAndSet(map, copy);
      if(updated)
        return deque;
    }
  }
	
	/**
	 * Deallocate the list of record batches
	 */
	public void deallocate(Collection<RecordBatch> batches) {
	  ByteBuffer[] buffers = new ByteBuffer[batches.size()];
	  int i = 0;
	  for(RecordBatch batch: batches) {
	    buffers[i] = batch.records.buffer();
	    i++;
	  }
	  free.deallocate(buffers);
	}
	
	/**
	 * Close this accumulator and force all the record buffers to be drained
	 */
	public void close() {
	  this.closed = true;
	}
	
}
