package kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.common.Cluster;
import kafka.common.Serializer;
import kafka.common.StringSerialization;
import kafka.common.TopicPartition;

/**
 * A Kafka producer that can be used to send data to the Kafka cluster. The producer
 * is thread safe and should be shared among all threads for best performance.
 */
public class KafkaProducer implements Producer {
  
  private final Partitioner partitioner = new DefaultPartitioner(); // TODO: should be pluggable
  private final Metadata metadata = new Metadata();
  private final RecordBuffers buffers;
  private final Serializer keySerializer = new StringSerialization();
  private final Serializer valSerializer = new StringSerialization();
  
  public KafkaProducer(Properties properties) {
    // TODO move to better config object
    int batchSize = Integer.parseInt(properties.getProperty("batch.size", Integer.toString(64*1024)));
    int totalSize = Integer.parseInt(properties.getProperty("batch.size", Integer.toString(5*1024*1024)));
    int lingerMs = Integer.parseInt(properties.getProperty("linger.ms", Integer.toString(0)));
    this.buffers = new RecordBuffers(batchSize, totalSize, lingerMs);
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
    this.metadata.update(Cluster.bootstrap(addresses));
  }
  
  @Override
  public RecordSend send(ProducerRecord record) {
    return send(record, null);
  }

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
      return buffers.append(tp, key, value, callback);
    } catch(InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    throw new RuntimeException("Implement me.");
  }

}
