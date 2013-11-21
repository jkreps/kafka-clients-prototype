package kafka.common.record;

public class LogEntry {
	
	private final long offset;
	private final LogRecord record;
	
	public LogEntry(long offset, LogRecord record) {
		this.offset = offset;
		this.record = record;
	}
	
	public long offset() {
		return this.offset;
	}
	
	public LogRecord record() {
		return this.record;
	}
}
