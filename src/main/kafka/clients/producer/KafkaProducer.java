package kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.clients.common.network.Selectable;
import kafka.clients.common.network.Selector;
import kafka.common.BytesSerialization;
import kafka.common.Cluster;
import kafka.common.KafkaException;
import kafka.common.Serializer;
import kafka.common.StringSerialization;
import kafka.common.TopicPartition;
import kafka.common.record.CompressionType;
import kafka.common.utils.KafkaThread;
import kafka.common.utils.SystemTime;
import kafka.common.utils.Time;

/**
 * A Kafka producer that can be used to send data to the Kafka cluster. The producer
 * is thread safe and should be shared among all threads for best performance.
 */
public class KafkaProducer implements Producer {
  
  private final Partitioner partitioner = new DefaultPartitioner(); // TODO: should be pluggable
  private final Metadata metadata = new Metadata();
  private final RecordAccumulator accumulator;
  private final Sender sender;
  private final Serializer keySerializer = new BytesSerialization();
  private final Serializer valSerializer = new BytesSerialization();
  private final Thread ioThread;
  
  public KafkaProducer(Properties properties) {
    // TODO: move to better config object
    // TODO: check property names 
    int batchSize = Integer.parseInt(properties.getProperty("batch.size", Integer.toString(64*1024)));
    int totalSize = Integer.parseInt(properties.getProperty("total.memory", Integer.toString(5*1024*1024)));
    int lingerMs = Integer.parseInt(properties.getProperty("linger.ms", Integer.toString(0)));
    int maxRequestSize = Integer.parseInt(properties.getProperty("max.request.size", Integer.toString(10*1024*1024)));
    int requestTimeout = Integer.parseInt(properties.getProperty("request.timeout", Integer.toString(30*1000)));
    int reconnectBackoff = Integer.parseInt(properties.getProperty("reconnect.backoff", Integer.toString(10)));
    String clientId = properties.getProperty("client.id", "");
    short numAcks = Short.parseShort(properties.getProperty("num.acks", "-1"));
    this.accumulator = new RecordAccumulator(batchSize, totalSize, lingerMs, new SystemTime());
    String[] urls = properties.getProperty("metadata.broker.list").split(",");
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    for(String url: urls) {
      if(url != null && url.trim().length() > 0) {
        String[] pieces = url.split(":");
        if(pieces.length != 2)
          throw new IllegalArgumentException("Invalid url in metadata.broker.list: " + url);
        try {
          // TODO: This actually does DNS resolution (?), which is a little weird
          addresses.add(new InetSocketAddress(pieces[0], Integer.parseInt(pieces[1])));
        } catch(NumberFormatException e) {
          throw new IllegalArgumentException("Invalid port in metadata.broker.list: " + url);
        }
      }
    }
    if(addresses.size() < 1)
      throw new IllegalArgumentException("No bootstrap urls given in metadata.broker.list.");
    this.metadata.update(Cluster.bootstrap(addresses), System.currentTimeMillis());
    this.sender = new Sender(new Selector(),
                             this.metadata,
                             this.accumulator,
                             clientId,
                             maxRequestSize,
                             reconnectBackoff,
                             numAcks,
                             requestTimeout,
                             new SystemTime());
    this.ioThread = new KafkaThread("kafka-network-thread", this.sender, false);
    this.ioThread.start();
  }
  
  /**
   * Equivalent to send(record, null)
   */
  @Override
  public RecordSend send(ProducerRecord record) {
    return send(record, null);
  }

  /**
   * Send a record.
   * 
   * The send is asynchronous and this method will return immediately.
   * 
   * The RecordSend returned by this call will hold the future response when it is ready. This object can be used to make the call synchronous:
   * <pre>
   *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
   *   producer.send(myRecord, null).await();
   * </pre>
   * 
   * @param record The record to send
   * @param callback A user-supplied callback to execute when the record has been acknowledged by the server
   * 
   */
  @Override
  public RecordSend send(ProducerRecord record, Callback callback) {
    Cluster cluster = metadata.fetch(record.topic());
    int partition = partitioner.partition(record, 
                                          cluster, 
                                          cluster.partitionsFor(record.topic()).size());
    byte[] key = keySerializer.toBytes(record.key());
    byte[] value = valSerializer.toBytes(record.value());
    try {
      TopicPartition tp = new TopicPartition(record.topic(), partition);
      return accumulator.append(tp, key, value, CompressionType.NONE, callback);
    } catch(InterruptedException e) {
      throw new KafkaException(e);
    }
  }

  /**
   * Close this producer. This method blocks until all in-flight requests complete.
   */
  @Override
  public void close() {
    this.sender.initiateClose();
    try {
      this.ioThread.join();
    } catch(InterruptedException e) {
      throw new KafkaException(e);
    }
  }

}
