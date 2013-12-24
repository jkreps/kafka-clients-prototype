package kafka.common.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

import kafka.common.utils.AbstractIterator;

public class MemoryRecords implements Records {
	
	private final ByteBuffer buffer;
	
	public MemoryRecords(int size) {
		this(ByteBuffer.allocate(size));
	}
	
	public MemoryRecords(ByteBuffer buffer) {
		this.buffer = buffer;
	}
	
	public void append(long offset, Record record) {
	    buffer.putLong(offset);
	    buffer.putInt(record.size());
	    buffer.put(record.buffer());
	    record.buffer().rewind();
	}

	public void append(long offset, byte[] key, byte[] value, CompressionType type) {
		buffer.putLong(offset);
		buffer.putInt(Record.recordSize(key.length, value.length));
		Record.write(this.buffer, key, value, type);
	}
	
	public boolean hasRoomFor(byte[] key, byte[] value) {
		return this.buffer.remaining() >= Records.LOG_OVERHEAD + Record.recordSize(key.length, value.length);
	}
	
	  /** Write the messages in this set to the given channel */
	  public int writeTo(GatheringByteChannel channel) throws IOException {
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
		  if(!other.getClass().equals(MemoryRecords.class))
			  return false;
		  MemoryRecords records = (MemoryRecords) other;
		  return this.buffer.equals(records.buffer);
	  }
		  
	  public int hashCode() {
		  return buffer.hashCode();
	  }
	  
	  public void clear() {
		  buffer.clear();
	  }
	  
	  public ByteBuffer buffer() {
	    ByteBuffer buffer = this.buffer.duplicate();
	    buffer.rewind();
	    return buffer;
	  }

    @Override
    public Iterator<LogEntry> iterator() {
      return new RecordsIterator(this.buffer);
    }
  
  /* TODO: allow reuse of the buffer used for iteration */
  public static class RecordsIterator extends AbstractIterator<LogEntry> {
    private final ByteBuffer buffer;
    
    public RecordsIterator(ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      copy.flip();
      this.buffer = copy;
    }

    @Override
    protected LogEntry makeNext() {
      if(buffer.remaining() < Records.LOG_OVERHEAD)
        return allDone();
      long offset = buffer.getLong();
      int size = buffer.getInt();
      if(size < 0)
        throw new IllegalStateException("Message with size " + size);
      if(buffer.remaining() < size)
        return allDone();
      ByteBuffer rec = buffer.slice();
      rec.limit(size);
      this.buffer.position(this.buffer.position() + size);
      return new LogEntry(offset, new Record(rec));
    }
  }
	
}
