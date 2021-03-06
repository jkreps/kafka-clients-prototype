package kafka.clients.producer.internals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.clients.producer.RecordSend;
import kafka.common.errors.TimeoutException;

/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordSend} instances that are batched together for
 * the same partition in the request.
 */
public final class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile long baseOffset = -1L;
    private volatile RuntimeException error;

    public ProduceRequestResult() {
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     * @param baseOffset
     * @param error
     */
    public void done(long baseOffset, RuntimeException error) {
        this.baseOffset = baseOffset;
        this.error = error;
        this.latch.countDown();
    }

    /**
     * Await the completion of this request
     */
    public void await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new TimeoutException("Interrupted while waiting for request to complete.");
        }
    }

    /**
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new TimeoutException("Interrupted while waiting for request to complete.");
        }
    }

    /**
     * The base offset for the request (the first offset in the message set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * Has the request completed?
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
