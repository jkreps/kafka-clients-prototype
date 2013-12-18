package kafka.common;

public interface Deserializer {

  public Object fromBytes(byte[] bytes);
  
}
