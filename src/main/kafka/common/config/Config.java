package kafka.common.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Config {
  
  private final Map<String, ConfigKey> configKeys = new HashMap<String, ConfigKey>();
  private final Map<String, Object> configs = new HashMap<String, Object>();
  
  public Config define(String name, Type type, Object defaultValue, Validator validator, String documentation) {
    configKeys.put(name, new ConfigKey(name, defaultValue, validator, documentation));
    return this;
  }
  
  public void parse(Properties p) {
    for(Object k: p.keySet()) {
      String name = (String) k;
      ConfigKey key = configKeys.get(name);
      throw new RuntimeException("Finish me");
    }
  }
  
  private Object parseType(String s, Type type) {
    switch(type) {
    case STRING:
      return s;
    case INT:
      return Integer.parseInt(s);
    case LONG:
      return Long.parseLong(s);
    case DOUBLE:
      return Double.parseDouble(s);
    default:
      throw new IllegalStateException("Unknown type.");
    }
  }

  public enum Type {
    STRING, INT, LONG, DOUBLE;
  }
  
  public interface Validator {
    public void ensureValid(Object o);
  }
  
  private class ConfigKey {
    public final String name;
    public final String documentation;
    public final Object defaultValue;
    public final Validator validator;
    
    public ConfigKey(String name, Object defaultValue, Validator validator, String documentation) {
      super();
      this.name = name;
      this.defaultValue = defaultValue;
      this.validator = validator;
      this.documentation = documentation;
    }
    
  }
}
