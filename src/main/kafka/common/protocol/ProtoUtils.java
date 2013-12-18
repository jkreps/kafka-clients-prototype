package kafka.common.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;
import kafka.common.protocol.types.Schema;
import kafka.common.protocol.types.Struct;

public class ProtoUtils {
  
  private static Schema schemaFor(Schema[][] schemas, int apiKey, int version) {
    if(apiKey < 0 || apiKey > schemas.length)
      throw new IllegalArgumentException("Invalid api key: " + apiKey);
    Schema[] versions = schemas[apiKey];
    if(version < 0 || version > versions.length)
      throw new IllegalArgumentException("Invalid version for API key " + apiKey + ": " + version);
    return versions[version];
  }
  
  public static short latestVersion(int apiKey) {
    if(apiKey < 0 || apiKey >= Protocol.CURR_VERSION.length)
      throw new IllegalArgumentException("Invalid api key: " + apiKey);
    return Protocol.CURR_VERSION[apiKey];
  }
  
  public static Schema requestSchema(int apiKey, int version) {
    return schemaFor(Protocol.REQUESTS, apiKey, version);
  }
  
  public static Schema currentRequestSchema(int apiKey) {
    return requestSchema(apiKey, latestVersion(apiKey));
  }
  
  public static Schema responseSchema(int apiKey, int version) {
    return schemaFor(Protocol.RESPONSES, apiKey, version);
  }
  
  public static Schema currentResponseSchema(int apiKey) {
    return schemaFor(Protocol.RESPONSES, apiKey, latestVersion(apiKey));
  }
  
  public static Struct parseRequest(int apiKey, int version, ByteBuffer buffer) {
    return (Struct) requestSchema(apiKey, version).read(buffer);
  }
  
  public static Struct parseResponse(int apiKey, ByteBuffer buffer) {
    return (Struct) currentResponseSchema(apiKey).read(buffer);
  }
  
  public static Cluster parseMetadataResponse(Struct response) {
    List<Node> brokers = new ArrayList<Node>();
    for (Struct broker : (Struct[]) response.get("brokers")) {
      int nodeId = (Integer) broker.get("node_id");
      String host = (String) broker.get("host");
      int port = (Integer) broker.get("port");
      brokers.add(new Node(nodeId, host, port));
    }
    List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
    for (Struct topicInfo : (Struct[]) response.get("topic_metadata")) {
      short topicError = (Short) topicInfo.get("topic_error_code");
      if (topicError == ErrorCodes.NO_ERROR) {
        String topic = (String) topicInfo.get("topic_name");
        for (Struct partitionInfo : (Struct[]) topicInfo.get("partition_metadata")) {
          short partError = (Short) partitionInfo.get("partition_error_code");
          if (partError == ErrorCodes.NO_ERROR) {
            int partition = (Integer) partitionInfo.get("partition_id");
            int leader = (Integer) partitionInfo.get("leader");
            int[] replicas = intArray((Integer[]) partitionInfo.get("replicas"));
            int[] isr = intArray((Integer[]) partitionInfo.get("isr"));
            partitions.add(new PartitionInfo(topic, partition, leader,replicas, isr));
          }
        }
      }
    }
    return new Cluster(brokers, partitions);
  }
  
  private static int[] intArray(Integer[] ints) {
    int[] copy = new int[ints.length];
    for(int i = 0; i < ints.length; i++)
      copy[i] = ints[i];
    return copy;
  }
}
