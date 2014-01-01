package kafka.common.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Config {
  
  private final Map<String, ConfigKey> configKeys = new HashMap<String, ConfigKey>();
  
  public Config define(String name, Type type, Object defaultValue, Validator validator, String documentation) {
    configKeys.put(name, new ConfigKey(name, type, defaultValue, validator, documentation));
    return this;
  }
  
  public Map<String, Object> parse(Properties p) {
    Map<String, Object> values = new HashMap<String, Object>();
    for(Object k: p.keySet()) {
      String name = (String) k;
      ConfigKey key = configKeys.get(name);
      Object value;
      if(p.contains(key))
        value = parseType(p.getProperty(key.name), key.type);
      else if(key.defaultValue != null)
        value = key.defaultValue;
      else
        throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
      values.put(key.name, value);
    }
    return values;
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
    public void ensureValid(String name, Object o);
  }
  
  public static class Range implements Validator {
    private final Number min;
    private final Number max;
    
    public Range(Number min, Number max) {
      this.min = min;
      this.max = max;
    }
    
    public void ensureValid(String name, Object o) {
      Number n = (Number) o;
      if(n.doubleValue() < min.doubleValue() || n.doubleValue() > max.doubleValue())
        throw new ConfigException(name, o, "Value must be in the range [" + min + ", " + max + "]");
    }
  }
  
  private static class ConfigKey {
    public final String name;
    public final Type type;
    public final String documentation;
    public final Object defaultValue;
    public final Validator validator;
    
    public ConfigKey(String name, Type type, Object defaultValue, Validator validator, String documentation) {
      super();
      this.name = name;
      this.type = type;
      this.defaultValue = defaultValue;
      this.validator = validator;
      if(this.validator != null)
        this.validator.ensureValid(name, defaultValue);
      this.documentation = documentation;
    }
    
  }
}
