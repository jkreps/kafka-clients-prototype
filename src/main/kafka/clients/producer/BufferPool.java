package kafka.clients.producer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. 
 * In particular it has the following properties:
 * 1. There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * 2. It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This prevents
 *    starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple buffers are
 *    deallocated.
 */
public final class BufferPool {
  
  private final long totalMemory;
  private final int poolableSize;
  private final ReentrantLock lock;
  private final Deque<ByteBuffer> free;
  private final Deque<Condition> waiters;
  private long available;

  public BufferPool(long memory, int poolableSize) {
    this.poolableSize = poolableSize;
    this.lock = new ReentrantLock();
    this.free = new ArrayDeque<ByteBuffer>();
    this.waiters = new ArrayDeque<Condition>();
    this.totalMemory = memory;
    this.available = memory;
  }
  
  /** the total free memory both unallocated and in the free list
   */
  public long availableMemory() {
    lock.lock();
    try {
      return this.available + this.free.size() * this.poolableSize;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Get the unallocated memory (not in the free list or in use)
   */
  public long unallocatedMemory() {
    lock.lock();
    try {
      return this.available;
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * The number of threads blocked waiting on memory
   */
  public int queued() {
    lock.lock();
    try {
      return this.waiters.size();
    } finally {
      lock.unlock();
    }
  }
  
  public int poolableSize() {
    return this.poolableSize;
  }
  
  public long totalMemory() {
    return this.totalMemory;
  }

  /**
   * Allocate a buffer of the given size
   * @param size The buffer size
   * @return The buffer
   * @throws InterruptedException If the thread is interrupted while blocked
   */
  public ByteBuffer allocate(int size) throws InterruptedException {
    if(size > this.totalMemory)
      throw new IllegalArgumentException("Attempt to allocate " + size + " bytes, but there is a hard limit of " + this.totalMemory + " on memory allocations.");
    
    this.lock.lock();
    try {
      // check if we have a free buffer of the right size pooled
  	  if(size == poolableSize && !this.free.isEmpty())
  	    return this.free.pollFirst();
  	  
  	  // now check if the request is immediately satisfiable with the memory on hand or if we need to block
  	  int freeListSize = this.free.size() * this.poolableSize;
  	  if(this.available + freeListSize >= size)  {
  	     // we have enough unallocated or pooled memory to immediately satisfy the request
  	    freeUp(size);
  	    this.available -= size;
	      lock.unlock();
	      return ByteBuffer.allocate(size);
	    } else {
	      // we are out of memory and will have to block
	      int accumulated = 0;
	      ByteBuffer buffer = null;
	      Condition moreMemory = this.lock.newCondition();
	      this.waiters.addLast(moreMemory);
	      // loop over and over until we have a buffer or have reserved enough memory to allocate one
	      while(accumulated < size) {
          moreMemory.await();
          // check if we can satisfy this request from the free list, otherwise allocate memory
          if(accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
            // just grab a buffer from the free list
            buffer = this.free.pollFirst();
            accumulated = size;
          } else {
            // we'll need to allocate memory, but we may only get part of what we need on this iteration
            freeUp(size - accumulated);
            int got = (int) Math.min(size - accumulated, this.available);
            this.available -= got;
            accumulated += got;
          }        
	      }
	      
	      // remove the condition for this thread to let the next thread in line start getting memory
	      Condition removed = this.waiters.removeFirst();
	      if(removed != moreMemory)
	        throw new IllegalStateException("Wrong condition: this shouldn't happen.");
	      
	      // signal any additional waiters if there is more memory left over for them
	      if(this.available > 0 || !this.free.isEmpty()) {
	        if(!this.waiters.isEmpty())
	          this.waiters.peekFirst().signal();
	      }
	      
	      // unlock and return the buffer
	      lock.unlock();
	      if(buffer == null)
	        return ByteBuffer.allocate(size);
	      else
	        return buffer;
      }
    } finally {
      if(lock.isHeldByCurrentThread())
	      lock.unlock();
    }
  }
  
  /**
   * Attempt to free up size worth of memory for allocation by deallocating pooled buffers (if needed)
   */
  private void freeUp(int size) {
    while(!this.free.isEmpty() && this.available < size)
      this.available += this.free.pollLast().limit();
  }
  
  /**
   * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the memory as free.
   * @param buffers The buffers to return
   */
  public void deallocate(ByteBuffer...buffers) {
    lock.lock();
    try {
      for(int i = 0; i < buffers.length; i++) {
        int size = buffers[i].limit();
        if(size == this.poolableSize)
          this.free.add(buffers[i]);
        else
          this.available += size;
        Condition moreMem = this.waiters.peekFirst();
        if(moreMem != null)
          moreMem.signal();
      }
    } finally {
      lock.unlock();
    }
  }
}