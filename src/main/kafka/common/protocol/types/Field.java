package kafka.common.protocol.types;

/**
 * A field in a schema
 */
public class Field {
	public final int index;
	public final String name;
	public final Type type;
	public final String doc;
	
	public Field(int index, String name, Type type, String doc) {
		this.index = index;
		this.name = name;
		this.type = type;
		this.doc = doc;
	}
	
	public Field(String name, Type type, String doc) {
		this(-1, name, type, doc);
	}
	
	public Field(String name, Type type) {
		this(name, type, "");
	}
	
}
