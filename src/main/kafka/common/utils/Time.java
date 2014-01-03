package kafka.common.utils;

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time
 */
public interface Time {

    /**
     * The current time in milliseconds
     */
    public long milliseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    public void sleep(long ms);

}
