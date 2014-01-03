package kafka.common.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigDef {

    private static final Object NO_DEFAULT_VALUE = new Object();

    private final Map<String, ConfigKey> configKeys = new HashMap<String, ConfigKey>();

    public ConfigDef define(String name,
                            Type type,
                            Object defaultValue,
                            Validator validator,
                            String documentation) {
        if (configKeys.containsKey(name))
            throw new ConfigException("Configuration " + name + " is defined twice.");
        Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
        configKeys.put(name, new ConfigKey(name, type, parsedDefault, validator, documentation));
        return this;
    }

    public ConfigDef define(String name,
                            Type type,
                            Object defaultValue,
                            String documentation) {
        return define(name, type, defaultValue, null, documentation);
    }

    public ConfigDef define(String name,
                            Type type,
                            Validator validator,
                            String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, validator, documentation);
    }

    public ConfigDef define(String name,
                            Type type,
                            String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, documentation);
    }

    public Map<String, Object> parse(Map<?, ?> props) {
        /* parse all known keys */
        Map<String, Object> values = new HashMap<String, Object>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            if (props.containsKey(key.name))
                value = parseType(key.name, props.get(key.name), key.type);
            else if (key.defaultValue == NO_DEFAULT_VALUE)
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            else
                value = key.defaultValue;
            values.put(key.name, value);
        }
        return values;
    }

    private Object parseType(String name,
                             Object value,
                             Type type) {
        try {
            String trimmed = null;
            if (value instanceof String)
                trimmed = ((String) value).trim();
            switch (type) {
                case BOOLEAN:
                    if (value instanceof String)
                        return Boolean.parseBoolean(trimmed);
                    else if (value instanceof Boolean)
                        return value;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                case STRING:
                    if (value instanceof String)
                        return trimmed;
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case INT:
                    if (value instanceof Integer) {
                        return (Integer) value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case LONG:
                    if (value instanceof Integer)
                        return ((Integer) value).longValue();
                    if (value instanceof Long)
                        return (Long) value;
                    else if (value instanceof String)
                        return Long.parseLong(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case DOUBLE:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof String)
                        return Double.parseDouble(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case LIST:
                    if (value instanceof List)
                        return (List<?>) value;
                    else if (value instanceof String)
                        return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
                    else
                        throw new ConfigException(name, value, "Expected a comma seperated list.");
                case CLASS:
                    if (value instanceof Class)
                        return (Class<?>) value;
                    else if (value instanceof String)
                        return Class.forName(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    public enum Type {
        BOOLEAN, STRING, INT, LONG, DOUBLE, LIST, CLASS;
    }

    public interface Validator {
        public void ensureValid(String name,
                                Object o);
    }

    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        public static Range atLeast(Number min) {
            return new Range(min, Double.MAX_VALUE);
        }

        public static Range between(Number min,
                                    Number max) {
            return new Range(min, max);
        }

        public void ensureValid(String name,
                                Object o) {
            Number n = (Number) o;
            if (n.doubleValue() < min.doubleValue() || n.doubleValue() > max.doubleValue())
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
            if (this.validator != null)
                this.validator.ensureValid(name, defaultValue);
            this.documentation = documentation;
        }

    }
}
