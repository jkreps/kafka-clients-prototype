package kafka.clients.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProduceRequestResult {
  
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile long baseOffset = -1L;
  private volatile short errorCode = -1;
  
  public ProduceRequestResult() {}
  
  public void done(long baseOffset, short errorCode) {
    this.baseOffset = baseOffset;
    this.errorCode = errorCode;
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
  
  public short errorCode() {
    return errorCode;
  }
}
