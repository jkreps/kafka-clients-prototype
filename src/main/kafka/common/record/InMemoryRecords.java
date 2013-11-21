package kafka.common.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class InMemoryRecords {
	
	private final ByteBuffer buffer;
	
	public InMemoryRecords(int size) {
		this(ByteBuffer.allocate(size));
	}
	
	public InMemoryRecords(ByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public void append(long offset, LogRecord record) {
	    buffer.putLong(offset);
	    buffer.putInt(record.size());
	    buffer.put(record.buffer());
	    record.buffer().rewind();
	}

	public void append(long offset, byte[] key, byte[] value, CompressionType type) {
		buffer.putLong(offset);
		buffer.putInt(LogRecord.recordSize(key.length, value.length));
		LogRecord.write(this.buffer, key, value, type);
	}
	
	public boolean hasRoomFor(byte[] key, byte[] value) {
		return this.buffer.remaining() >= Records.LOG_OVERHEAD + LogRecord.recordSize(key.length, value.length);
	}
	
	  /** Write the messages in this set to the given channel */
	  public int writeTo(WritableByteChannel channel) throws IOException {
	    return channel.write(buffer);
	  }
	  
	  public int sizeInBytes() {
		  return this.buffer.limit();
	  }
		  
	  public boolean equals(Object other) {
		  if(this == other)
			  return true;
		  if(other == null)
			  return false;
		  if(!other.getClass().equals(InMemoryRecords.class))
			  return false;
		  InMemoryRecords records = (InMemoryRecords) other;
		  return this.buffer.equals(records.buffer);
	  }
		  
	  public int hashCode() {
		  return buffer.hashCode();
	  }
	  
	  public void clear() {
		  buffer.clear();
	  }
	
}
