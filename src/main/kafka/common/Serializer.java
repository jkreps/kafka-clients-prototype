package kafka.common;

public interface Serializer {

  public byte[] toBytes(Object o);
  
}
