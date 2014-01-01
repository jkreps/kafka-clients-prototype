package kafka.clients.tools;

import java.util.Arrays;
import java.util.Properties;

import kafka.clients.producer.Callback;
import kafka.clients.producer.KafkaProducer;
import kafka.clients.producer.ProducerRecord;
import kafka.clients.producer.RecordSend;

public class ProducerPerformance {

  public static void main(String[] args) throws Exception {
    if(args.length != 3) {
      System.err.println("USAGE: java " + ProducerPerformance.class.getName() + " url num_messages message_size");
      System.exit(1);
    }
    String url = args[0];
    int numMessages = Integer.parseInt(args[1]);
    int messageSize = Integer.parseInt(args[2]);
    Properties props = new Properties();
    props.setProperty("num.acks", "1");
    //props.setProperty("linger.ms", "5");
    props.setProperty("metadata.broker.list", url);
    props.setProperty("request.timeout", Integer.toString(Integer.MAX_VALUE));
    
    KafkaProducer producer = new KafkaProducer(props);
    Callback callback = new Callback() {
      public void onCompletion(RecordSend send) {
        try {
          send.offset();
        } catch(Exception e) {
          e.printStackTrace();
        }
      }
    };
    byte[] payload = new byte[messageSize];
    Arrays.fill(payload, (byte) 1);
    ProducerRecord record = new ProducerRecord("test", payload);
    long start = System.currentTimeMillis();
    for(int i = 0; i < numMessages; i++) {
      producer.send(record, null);
      if(i % 1000000 == 0)
        System.out.println(i);
    }
    long ellapsed = System.currentTimeMillis() - start;
    double msgsSec = 1000.0 * numMessages / (double) ellapsed;
    double mbSec = msgsSec * messageSize / (1024.0*1024.0);
    System.out.printf("%d messages sent in %d ms ms. %.2f messages per second (%.2f mb/sec).", numMessages, ellapsed, msgsSec, mbSec);
    producer.close();
  }
  
}
