package kafka.clients.producer.internals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.common.errors.TimeoutException;

public final class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile long baseOffset = -1L;
    private volatile RuntimeException error;

    public ProduceRequestResult() {
    }

    public void done(long baseOffset, RuntimeException error) {
        this.baseOffset = baseOffset;
        this.error = error;
        this.latch.countDown();
    }

    public void await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new TimeoutException("Interrupted while waiting for request to complete.");
        }
    }

    public boolean await(long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new TimeoutException("Interrupted while waiting for request to complete.");
        }
    }

    public long baseOffset() {
        return baseOffset;
    }

    public RuntimeException error() {
        return error;
    }

    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
