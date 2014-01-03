package kafka.common;

/**
 * A class that controls how an object is turned into bytes. Classes implementing this interface will generally be
 * instantiated by the framework.
 * <p>
 * An implementation may or may not handle nulls.
 * <p>
 * An implementation that requires special configuration parameters can implement Configurable
 */
public interface Serializer {

    /**
     * Translate an object into bytes
     */
    public byte[] toBytes(Object o);

}
