package kafka.clients.producer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.record.InMemoryRecords;

/**
 * A collection of queues that maintain a global size across all the queues. When this size reaches a maximum new adds block.
 */
public class RecordBuffers {
	
	private final int batchSize;
	private final AtomicInteger remainingMemory;
	private final long lingerMs;
	private final ConcurrentMap<TopicPartition, Deque<RecordBuffer>> buffers;
	private final BlockingQueue<RecordBuffer> free;
	
	public RecordBuffers(int batchSize, int totalSize, long lingerMs) {
		this.batchSize = batchSize;
		this.remainingMemory = new AtomicInteger(totalSize);
		this.lingerMs = lingerMs;
		this.buffers = new ConcurrentHashMap<TopicPartition, Deque<RecordBuffer>>();
		this.free = new ArrayBlockingQueue<RecordBuffer>(totalSize / batchSize + 1);
	}
	
	public CountDownLatch append(TopicPartition tp, byte[] key, byte[] value) throws InterruptedException {
		// check if we have an in-progress buffer
		Deque<RecordBuffer> dq = dequeFor(tp);
		RecordBuffer buffer = null;
		synchronized(dq) {
			buffer = dq.peekFirst();
			if(buffer != null && !buffer.records.hasRoomFor(key, value)) {
				buffer.full = true;
				buffer = null;
			}
			// if we don't have one active, but we have one just sitting around, take it
			if(buffer == null) {
				buffer = free.poll();
				dq.addFirst(buffer);
			}
			
			// if not try to make one
			if(buffer == null) {
				buffer = tryCreateBuffer(tp, this.batchSize);
				dq.addFirst(buffer);
			}
			
			// if we got a buffer do the append
			if(buffer != null) {
				buffer.append(key, value);
				return buffer.latch;
			}
		}
		
		// okay we failed to get a buffer without blocking, wait for a buffer to free up
		buffer = free.take();
		synchronized(dq) {
			RecordBuffer first = dq.peekFirst();
			if(first != null && first.records.hasRoomFor(key, value)) {
				// somebody else found us a buffer, return the one we waited for!
				free.add(buffer);
				buffer = first;
			} else {
				dq.addFirst(buffer);
			}
			buffer.append(key, value);			
		} 
			
		return buffer.latch;
	}
	
	public List<TopicPartition> ready(long now) {
		List<TopicPartition> ready = new ArrayList<TopicPartition>();
		for(Map.Entry<TopicPartition, Deque<RecordBuffer>> entry: this.buffers.entrySet()) {
			Deque<RecordBuffer> deque = entry.getValue();
			synchronized(deque) {
				RecordBuffer buffer = deque.peekFirst();
				if(buffer != null && buffer.ready(now))
					ready.add(buffer.tp);
			}
		}
		return ready;
	}
	
	public List<RecordBuffer> drain(List<TopicPartition> partitions) {
		List<RecordBuffer> ready = new ArrayList<RecordBuffer>();
		for(TopicPartition tp: partitions) {
			Deque<RecordBuffer> deque = this.buffers.get(tp);
			if(deque != null) {
				synchronized(deque) {
					RecordBuffer buffer = deque.peekFirst();
					if(buffer != null)
						ready.add(deque.pollFirst());
				}
			}
		}
		return ready;
	}
	
	/**
	 * Create the buffer only if we have enough remaining memory available
	 * @return The buffer or null if there wasn't room
	 */
	private RecordBuffer tryCreateBuffer(TopicPartition tp, int size) {
		while(true) {
			int remaining = this.remainingMemory.get();
			if(remaining < size)
				return null;
			boolean set = remainingMemory.compareAndSet(remaining, remaining - size);
			if(set)
				return new RecordBuffer(tp, new InMemoryRecords(size));
		}
	}
	
	private Deque<RecordBuffer> dequeFor(TopicPartition tp) {
		Deque<RecordBuffer> d = this.buffers.get(tp);
		if(d == null) {
			this.buffers.putIfAbsent(tp, new ArrayDeque<RecordBuffer>());
			d = this.buffers.get(tp);
		}
		return d;
	}
	
	public void donate(List<RecordBuffer> buffers) {
		free.addAll(buffers);
	}
	
	class RecordBuffer {
		int size = 0;
		boolean full = false;
		final long begin;
		final CountDownLatch latch;
		final InMemoryRecords records;
		final TopicPartition tp;
		
		public RecordBuffer(TopicPartition tp, InMemoryRecords records) {
			this.begin = System.currentTimeMillis();
			this.records = records;
			this.tp = tp;
			this.latch = new CountDownLatch(1);
		}

		public void append(byte[] key, byte[] value) {
			this.size += 1;
			this.records.append(0L, key, value, CompressionType.NONE);
		}
		
		public boolean ready(long now) {
			return full || now - begin > lingerMs;
		}
	}
	
}
