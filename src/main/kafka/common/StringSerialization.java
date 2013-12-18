package kafka.common;

import java.io.UnsupportedEncodingException;

public class StringSerialization implements Serializer, Deserializer {
  
  private final String encoding;
  
  public StringSerialization(String encoding) {
    super();
    this.encoding = encoding;
  }
  
  public StringSerialization() {
    this("UTF8");
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    if(bytes == null) {
      return null;
    } else {
      try {
        return new String(bytes, encoding);
      } catch(UnsupportedEncodingException e) {
        throw new KafkaException(e);
      }
    }
  }

  @Override
  public byte[] toBytes(Object o) {
    if(o == null) {
      return null;
    } else {
      try {
        return ((String) o).getBytes(encoding);
      } catch(UnsupportedEncodingException e) {
        throw new KafkaException(e);
      }
    }
  }

}
