package kafka.common.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractConfig {
  
  private final Set<String> used;
  private final Map<String, Object> configs;
  
  public AbstractConfig(Map<String, Object> configs) {
    this.configs = configs;
    this.used = Collections.synchronizedSet(new HashSet<String>());
  }
  
  protected Object get(String key) {
    if(!configs.containsKey(key))
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    used.add(key);
    return configs.get(key);
  }
  
  public int getInt(String key) {
    return (Integer) get(key);
  }
  
  public long getLong(String key) {
    return (Long) get(key);
  }
  
  @SuppressWarnings("unchecked")
  public List<String> getList(String key) {
    return (List<String>) get(key);
  }
  
  public String getString(String key) {
    return (String) get(key);
  }

  public Class<?> getClass(String key) {
    return (Class<?>) get(key);
  }
  
  public Set<String> unused() {
    Set<String> keys = new HashSet<String>(configs.keySet());
    keys.remove(used);
    return keys;
  }
  
}
