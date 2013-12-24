package kafka.common.record;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * A binary format which consists of a 4 byte size, an 8 byte offset, and the record bytes
 */
public interface Records extends Iterable<LogEntry> {
	
	int SIZE_LENGTH = 4;
	int OFFSET_LENGTH = 8;
	int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;

	public int writeTo(GatheringByteChannel channel) throws IOException;
	
	public int sizeInBytes();
	
}
