package kafka.clients.producer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.common.Cluster;
import kafka.common.PartitionInfo;

public final class Metadata {
  
  private Cluster cluster = Cluster.empty();
  private boolean needsUpdate = false;
  private final Set<String> topics = new HashSet<String>();

  public synchronized Cluster fetch(String topic) {
    List<PartitionInfo> partitions = null;
    do {     
      partitions = cluster.partitionsFor(topic);
      if(partitions == null) {
        topics.add(topic);
        needsUpdate = true;
        try {
          wait();
        } catch(InterruptedException e) { /* this is fine, just try again */ }
      } else {
        return cluster;
      }
    } while(true);
  }
  
  public synchronized boolean needsUpdate() {
      return this.needsUpdate;
  }
  
  public synchronized Set<String> topics() {
    return new HashSet<String>(this.topics);
  }
  
  public synchronized void update(Cluster cluster) {
    this.needsUpdate = false;
    this.cluster = cluster;
    notifyAll();
  }
  
}
