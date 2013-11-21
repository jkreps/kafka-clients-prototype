package kafka.common.protocol.types;

import java.nio.ByteBuffer;

/**
 * Represents a type for an array of a particular type
 */
public class ArrayOf extends Type {

	private final Type type;
	
	public ArrayOf(Type type) {
		this.type = type;
	}

	public void write(ByteBuffer buffer, Object o) {
		Object[] objs = (Object[]) o;
		int size = objs.length;
		buffer.putInt(size);
		for(int i = 0; i < size; i++)
			type.write(buffer, objs[i]);
	}

	public Object read(ByteBuffer buffer) {
		int size = buffer.getInt();
		Object[] objs = new Object[size];
		for(int i = 0; i < size; i++)
			objs[i] = type.read(buffer);
		return objs;
	}
	
	public int sizeOf(Object o) {
		Object[] objs = (Object[]) o;
		if(objs.length == 0)
			return 4;
		else
			return 4 + objs.length * type.sizeOf(objs[0]);
	}
	
}
