package kafka.common;

/**
 * A class that controls how an object is turned into bytes. Classes implementing this interface will generally be
 * instantiated by the framework.
 * <p>
 * An implementation that requires special configuration parameters can implement Initable
 */
public interface Deserializer {

    /**
     * Map a byte[] to an object
     */
    public Object fromBytes(byte[] bytes);

}
