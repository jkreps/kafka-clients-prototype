package kafka.clients.producer;

import java.util.concurrent.TimeUnit;

import kafka.common.errors.TimeoutException;

/**
 * An asynchronously computed response from sending a record. Calling <code>await()</code> will block until the
 * response for this record is available.
 */
public class RecordSend {
	
	private final long relativeOffset;
	private final ProduceRequestResult result;
	
	RecordSend(long relativeOffset, ProduceRequestResult result) {
	  this.relativeOffset = relativeOffset;
		this.result = result;
	}
	
	/**
	 * Block until this send is complete
   * @return the same object for chaining
	 */
	public RecordSend await() {
	  try {
  		result.await();
  		result.error().maybeThrow();
      return this;
    } catch(InterruptedException e) {
      throw new TimeoutException("Request was interrupted.", e);
    }
	}
	
	/**
	 * Block until this send is complete or the given timeout ellapses
	 * @param timeout the time to wait
	 * @param unit the units of the time
	 * @return the same object for chaining
	 */
	public RecordSend await(long timeout, TimeUnit unit) {
	  try {
  		boolean success = result.await(timeout, unit);
  		if(!success)
  		  throw new TimeoutException("Request did not complete after " + timeout + " " + unit);
  		result.error().maybeThrow();
  		return this;
	  } catch(InterruptedException e) {
	    throw new TimeoutException("Request was interrupted.", e);
	  }
	}
	
	/**
	 * Get the offset for the given message. This method will block until the request is complete
	 */
	public long offset() {
		await();
		return this.result.baseOffset() + this.relativeOffset;
	}
	
	/**
	 * Check if the request is complete without blocking
	 */
	public boolean completed() {
	  return this.result.completed();
	}
	
}
