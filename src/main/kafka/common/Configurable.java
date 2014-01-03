package kafka.common;

import java.util.Map;

public interface Configurable {

  public void configure(Map<String, ?> config);
  
}
