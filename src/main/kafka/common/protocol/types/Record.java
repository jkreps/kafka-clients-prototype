package kafka.common.protocol.types;

import java.util.Arrays;

/**
 * A record that can be serialized and deserialized according to a pre-defined schema
 */
public final class Record {
	private final Schema schema;
	private final Object[] values;
	
	Record(Schema schema, Object[] values) {
		this.schema = schema;
		this.values = values;
	}
	
	public Record(Schema schema) {
		this.schema = schema;
		this.values = new Object[this.schema.numFields()];
	}
	
	/**
	 * The schema for this record.
	 */
	public Schema schema() {
		return this.schema;
	}
	
	/**
	 * Get the record value for the field directly by the field index with no lookup needed (faster!)
	 * @param field The field to look up
	 * @return The value for that field.
	 */
	public Object get(Field field) {
		if(field.index > values.length)
			throw new IllegalArgumentException("Invalid field index: " + field.index);
		return this.values[field.index];
	}
	
	/**
	 * Get the record value for the field with the given name by doing a hash table lookup (slower!)
	 * @param name The name of the field
	 * @return The value in the record
	 */
	public Object get(String name) {
		Field field = schema.get(name);
		if(field == null)
			throw new IllegalArgumentException("No such field: " + name);
		return this.values[field.index];
	}
	
	/**
	 * Set the given field to the specified value
	 * @param field The field
	 * @param value The value
	 */
	public Record set(Field field, Object value) {
		this.values[field.index] = value;
		return this;
	}
	
	/**
	 * Set the field specified by the given name to the value
	 * @param name The name of the field
	 * @param value The value to set
	 */
	public Record set(String name, Object value) {
		Field field = this.schema.get(name);
		if(field == null)
			throw new IllegalArgumentException("Unknown field: " + name);
		set(field, value);
		return this;
	}
	
	/**
	 * Empty all the values from this record
	 */
	public void clear() {
		Arrays.fill(this.values, null);
	}
}
