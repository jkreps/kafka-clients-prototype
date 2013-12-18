package kafka.common.protocol.types;

import java.nio.ByteBuffer;

import kafka.common.utils.Utils;

/**
 * A serializable type
 */
public abstract class Type {

  public abstract void write(ByteBuffer buffer, Object o);
  public abstract Object read(ByteBuffer buffer);
  public abstract int sizeOf(Object o);
    	
	public static final Type INT8 = new Type() {
		public void write(ByteBuffer buffer, Object o) {
			buffer.put((Byte) o);
		}
		public Object read(ByteBuffer buffer) {
			return buffer.get();
		}
		public int sizeOf(Object o) {
			return 1;
		}
		public String toString() {
		  return "INT8";
		}
	};
	
	public static final Type INT16 = new Type() {
		public void write(ByteBuffer buffer, Object o) {
			buffer.putShort((Short) o);
		}
		public Object read(ByteBuffer buffer) {
			return buffer.getShort();
		}
		public int sizeOf(Object o) {
			return 2;
		}
    public String toString() {
      return "INT16";
    }
	};
	
	public static final Type INT32 = new Type() {
		public void write(ByteBuffer buffer, Object o) {
			buffer.putInt((Integer) o);
		}
		public Object read(ByteBuffer buffer) {
			return buffer.getInt();
		}
		public int sizeOf(Object o) {
			return 4;
		}
    public String toString() {
      return "INT32";
    }
	};
	
	public static final Type INT64 = new Type() {
		public void write(ByteBuffer buffer, Object o) {
			buffer.putLong((Long) o);
		}
		public Object read(ByteBuffer buffer) {
			return buffer.getLong();
		}
		public int sizeOf(Object o) {
			return 8;
		}
    public String toString() {
      return "INT64";
    }
	};
	
	public static final Type STRING = new Type() {
		public void write(ByteBuffer buffer, Object o) {
			byte[] bytes = Utils.utf8((String) o);
			if(bytes.length > Short.MAX_VALUE)
				throw new IllegalArgumentException("String is longer than the maximum string length.");
			buffer.putShort((short) bytes.length);
			buffer.put(bytes);
		}
		public Object read(ByteBuffer buffer) {
			int length = buffer.getShort();
			byte[] bytes = new byte[length];
			buffer.get(bytes);
			return Utils.utf8(bytes);
		}
		public int sizeOf(Object o) {
			return 2 + Utils.utf8Length((String) o);
		}
    public String toString() {
      return "STRING";
    }
	};
	
	public static final Type BYTES = new Type() {
	  public void write(ByteBuffer buffer, Object o) {
	    ByteBuffer arg = (ByteBuffer) o;
	    int pos = arg.position();
	    buffer.put(arg);
	    arg.position(pos);
	  }
	  public Object read(ByteBuffer buffer) {
	    ByteBuffer val = buffer.duplicate();
	    int size = val.getInt();
	    val.limit(size);
	    return val;
	  }
	  public int sizeOf(Object o) {
	    ByteBuffer buffer = (ByteBuffer) o;
	    return buffer.remaining();
	  }
	  public String toString() {
	    return "BYTES";
	  }
	};

}
