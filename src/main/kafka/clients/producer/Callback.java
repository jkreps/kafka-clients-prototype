package kafka.clients.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback will execute in the
 * background I/O thread so it should be fast.
 */
public interface Callback {
   public void onCompletion(RecordSend send);
}
