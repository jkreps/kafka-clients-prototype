package kafka.clients.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The response from sending a record
 */
public class RecordSend {
	
	private final CountDownLatch latch;
	private volatile long offset;
	private volatile int errorCode;
	private volatile Throwable throwable;
	private volatile Callback callback;
	private final AtomicBoolean callbackExecuted;
	
	RecordSend(CountDownLatch latch) {
		this.latch = latch;
		this.callbackExecuted = new AtomicBoolean(false);
	}
	
	public void await() throws InterruptedException {
		latch.await();
	}
	
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return latch.await(timeout, unit);
	}
	
	void done(long offset, Throwable throwable, int errorCode) {
		this.offset = offset;
		this.throwable = throwable;
		this.errorCode = errorCode;
		this.latch.countDown();
		if(callbackExecuted.compareAndSet(false, true) && callback != null)
			callback.onCompletion(this);
	}
	
	public long offset() throws InterruptedException {
		latch.await();
		return this.offset;
	}
	
	public void doAfter(Callback callback) {
		this.callback = callback;
		if(this.callbackExecuted.compareAndSet(false, true))
			callback.onCompletion(this);
	}
	
	public interface Callback {
		public void onCompletion(RecordSend send);
	}
}
