package kafka.clients.producer;

import java.util.concurrent.CountDownLatch;

public class SendResponse {

	private final long offset;
	private final CountDownLatch latch;
	
	public SendResponse(long offset, CountDownLatch latch) {
		this.offset = offset;
		this.latch = latch;
	}

	public long offset() {
		return this.offset;
	}
	
	public CountDownLatch latch() {
		return this.latch;
	}
	
}
