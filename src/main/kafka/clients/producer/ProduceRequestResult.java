package kafka.clients.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.common.protocol.Errors;

public class ProduceRequestResult {
  
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile long baseOffset = -1L;
  private volatile Errors error;
  
  public ProduceRequestResult() {}
  
  public void done(long baseOffset, short errorCode) {
    this.baseOffset = baseOffset;
    this.error = Errors.forCode(errorCode);
    this.latch.countDown();
  }
  
  public void await() throws InterruptedException {
    latch.await();
  }
  
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return latch.await(timeout, unit);
  }
  
  public long baseOffset() {
    return baseOffset;
  }
  
  public Errors error() {
    return error;
  }
  
  public boolean completed() {
    return this.latch.getCount() == 0L;
  }
}
