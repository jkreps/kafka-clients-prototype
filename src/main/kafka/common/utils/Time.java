package kafka.common.utils;

public interface Time {

  public long milliseconds();
  
  public void sleep(long ms);
  
}
