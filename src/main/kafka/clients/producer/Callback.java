package kafka.clients.producer;

public interface Callback {
   public void onCompletion(RecordSend send);
}
