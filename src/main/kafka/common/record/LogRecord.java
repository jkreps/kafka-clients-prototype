package kafka.common.record;

import java.nio.ByteBuffer;

import kafka.common.utils.Utils;

public class LogRecord {

	  /**
	   * The current offset and size for all the fixed-length fields
	   */
	  public static final int CrcOffset = 0;
	  public static final int CrcLength = 4;
	  public static final int MagicOffset = CrcOffset + CrcLength;
	  public static final int MagicLength = 1;
	  public static final int AttributesOffset = MagicOffset + MagicLength;
	  public static final int AttributesLength = 1;
	  public static final int KeySizeOffset = AttributesOffset + AttributesLength;
	  public static final int KeySizeLength = 4;
	  public static final int KeyOffset = KeySizeOffset + KeySizeLength;
	  public static final int ValueSizeLength = 4;
	  
	  /** The amount of overhead bytes in a record */
	  public static final int RECORD_OVERHEAD = KeyOffset + ValueSizeLength;
	  
	  /**
	   * The minimum valid size for the record header
	   */
	  public static final int MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;
	  
	  /**
	   * The current "magic" value
	   */
	  public static final byte CurrentMagicValue = 0;

	  /**
	   * Specifies the mask for the compression code. 2 bits to hold the compression codec.
	   * 0 is reserved to indicate no compression
	   */
	  public static final int CompressionCodeMask = 0x03; 

	  /**
	   * Compression code for uncompressed records
	   */
	  public static final int NoCompression = 0;
	  
	  private final ByteBuffer buffer;
	  
	  public LogRecord(ByteBuffer buffer) {
		  this.buffer = buffer;
	  }
	  
	  /**
	   * A constructor to create a LogRecord
	   * @param key The key of the record (null, if none)
	   * @param value The record value
	   * @param codec The compression codec used on the contents of the record (if any)
	   * @param valueOffset The offset into the payload array used to extract payload
	   * @param valueSize The size of the payload to use
	   */
	  public LogRecord(byte[] key, 
			           byte[] value,            
			           CompressionType codec, 
			           int valueOffset, 
	                   int valueSize) {
	    this(ByteBuffer.allocate(recordSize(key == null? 0: key.length, 
	    		                            valueSize >= 0? valueSize : value.length - valueOffset)));
	    write(this.buffer, key, value, codec, valueOffset, valueSize);
	    this.buffer.reset();
	  }
	  
	  public LogRecord(byte[] key, byte[] value, CompressionType codec) {
	    this(key, value, codec, 0, -1);
	  }
	  
	  public LogRecord(byte[] value, CompressionType codec) {
	    this(null, value, codec);
	  }
	  
	  public LogRecord(byte[] key, byte[] value) {
	    this(key, value, CompressionType.NONE);
	  }
	    
	  public LogRecord(byte[] value) {
	    this(null, value, CompressionType.NONE);
	  }
	  
	  public static void write(ByteBuffer buffer,
			                   byte[] key, 
	                           byte[] value,            
	                           CompressionType codec, 
	                           int valueOffset, 
                               int valueSize) {
		    // skip crc, we will fill that in at the end
		  int pos = buffer.position();
		    buffer.position(pos + MagicOffset);
		    buffer.put(CurrentMagicValue);
		    byte attributes = 0;
		    if (codec.id > 0)
		      attributes =  (byte) (attributes | (CompressionCodeMask & codec.id));
		    buffer.put(attributes);
		    if (key == null) {
		      buffer.putInt(-1);
		    } else {
		      buffer.putInt(key.length);
		      buffer.put(key, 0, key.length);
		    }
		    int size = (valueSize >= 0)? valueSize : (value.length - valueOffset);
		    buffer.putInt(size);
		    buffer.put(value, valueOffset, size);
		    
		    // now compute the checksum and fill it in
		    long crc = computeChecksum(buffer, buffer.arrayOffset() + MagicOffset, buffer.limit() - MagicOffset);
		    Utils.writeUnsignedInt(buffer, pos + CrcOffset, crc);
	  }
	  
	  public static void write(ByteBuffer buffer, byte[] key, byte[] value, CompressionType codec) {
		  write(buffer, key, value, codec, 0, value.length);
	  }
	  
	  public static int recordSize(int keySize, int valueSize) {
		  return CrcLength + MagicLength + AttributesLength + KeySizeLength + keySize + ValueSizeLength + valueSize;
	  }
	  
	  public ByteBuffer buffer() {
		  return this.buffer;
	  }
	    
	  /**
	   * Compute the checksum of the record from the record contents
	   */
	  public static long computeChecksum(ByteBuffer buffer, int position, int limit) { 
	    return Utils.crc32(buffer.array(), buffer.arrayOffset() + position + MagicOffset,  limit - MagicOffset);
	  }
	  
	  /**
	   * Compute the checksum of the record from the record contents
	   */
	  public long computeChecksum() {
		  return computeChecksum(buffer, 0, buffer.limit());
	  }
	  
	  /**
	   * Retrieve the previously computed CRC for this record
	   */
	  public long checksum() {
		  return Utils.readUnsignedInt(buffer, CrcOffset);
	  }
	  
	    /**
	   * Returns true if the crc stored with the record matches the crc computed off the record contents
	   */
	  public boolean isValid() {
		  return checksum() == computeChecksum();
	  }
	  
	  /**
	   * Throw an InvalidMessageException if isValid is false for this record
	   */
	  public void ensureValid() {
	    if(!isValid())
	      throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
	  }
	  
	  /**
	   * The complete serialized size of this record in bytes (including crc, header attributes, etc)
	   */
	  public int size() {
		  return buffer.limit();
	  }
	  
	  /**
	   * The length of the key in bytes
	   */
	  public int keySize() {
		  return buffer.getInt(KeySizeOffset);
	  }
	  
	  /**
	   * Does the record have a key?
	   */
	  public boolean hasKey() {
		  return keySize() >= 0;
	  }
	  
	  /**
	   * The position where the value size is stored
	   */
	  private int valueSizeOffset() {
		  return KeyOffset + Math.max(0, keySize());
	  }
	  
	  /**
	   * The length of the value in bytes
	   */
	  public int valueSize() {
		  return buffer.getInt(valueSizeOffset());
	  }
	  
	  /**
	   * The magic version of this record
	   */
	  public byte magic() { 
		  return buffer.get(MagicOffset);
	  }
	  
	  /**
	   * The attributes stored with this record
	   */
	  public byte attributes() {
		  return buffer.get(AttributesOffset);
	  }
	  
	  /**
	   * The compression codec used with this record
	   */
	  public CompressionType compressionType() {
		  return CompressionType.forId(buffer.get(AttributesOffset) & CompressionCodeMask);
	  }
	  
	  /**
	   * A ByteBuffer containing the value of this record
	   */
	  public ByteBuffer value(){
		  return sliceDelimited(valueSizeOffset());
	  }
	  
	  /**
	   * A ByteBuffer containing the message key
	   */
	  public ByteBuffer key() {
		  return sliceDelimited(KeySizeOffset);
	  }
	  
	  /**
	   * Read a size-delimited byte buffer starting at the given offset
	   */
	  private ByteBuffer sliceDelimited(int start) {
	    int size = buffer.getInt(start);
	    if(size < 0) {
	      return null;
	    } else {
	      ByteBuffer b = buffer.duplicate();
	      b.position(start + 4);
	      b = b.slice();
	      b.limit(size);
	      b.rewind();
	      return b;
	    }
	  }

	  public String toString() {
	    return String.format("Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)", magic(), attributes(), checksum(), key(), value());
	  }
	  
	  public boolean equals(Object other) {
		  if(this == other)
			  return true;
		  if(other == null)
			  return false;
		  if(!other.getClass().equals(LogRecord.class))
			  return false;
		  LogRecord record = (LogRecord) other;
		  return this.buffer.equals(record.buffer);
	  }
	  
	  public int hashCode() {
		  return buffer.hashCode();
	  }
	
}
