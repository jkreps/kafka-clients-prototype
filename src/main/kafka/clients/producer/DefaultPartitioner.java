package kafka.clients.producer;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.Cluster;
import kafka.common.utils.Utils;

public class DefaultPartitioner implements Partitioner {
  
  private final AtomicInteger counter = new AtomicInteger(0);

  @Override
  public int partition(Object key, Object value, Cluster cluster, int numPartitions) {
    if(key == null) {
      int next = counter.getAndIncrement();
      return Utils.abs(next) % numPartitions;
    } else {
      return Utils.abs(key.hashCode()) % numPartitions;
    }
  }

}
