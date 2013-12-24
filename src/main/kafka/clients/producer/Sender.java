package kafka.clients.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.clients.common.network.Selectable;
import kafka.clients.common.network.NetworkReceive;
import kafka.clients.common.network.NetworkSend;
import kafka.clients.common.network.Send;
import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.TopicPartition;
import kafka.common.protocol.ApiKeys;
import kafka.common.protocol.Errors;
import kafka.common.protocol.ProtoUtils;
import kafka.common.protocol.types.ArrayOf;
import kafka.common.protocol.types.Schema;
import kafka.common.protocol.types.Struct;
import kafka.common.requests.RequestHeader;
import kafka.common.requests.RequestSend;
import kafka.common.requests.ResponseHeader;
import kafka.clients.producer.RecordBuffers.RecordBuffer;

/**
 * Partition states: RESOLVING, CONNECTING, CONNECTED
 *
 */
public class Sender implements Runnable {
	
	private static enum NodeState {CONNECTING, CONNECTED}
	
	private static Comparator<RecordBuffer> TOPIC_COMPARATOR = new Comparator<RecordBuffer>() {
    @Override
    public int compare(RecordBuffer b1, RecordBuffer b2) {
      return b1.tp.topic().compareTo(b2.tp.topic());
    }
  };
	
	private final Map<Integer, NodeState> nodeState;
	private final RecordBuffers buffers;
	private final Selectable selector;
	private final String clientId;
	private final int maxBatchSize;
	private final long lingerMs;
	private final short acks;
	private final int requestTimeout;
	private final InFlightRequests inFlightRequests;
	private long lastMetadataFetch = 0L;
	private Cluster cluster;
	private Metadata metadata;
	private int correlation = 0;
	private volatile boolean running;

	public Sender(Selectable selector,
	              Metadata metadata,
			          RecordBuffers buffers,
			          String clientId,
			          int maxBatchSize, 
			          long lingerMs,
			          short acks,
			          int requestTimeout) {
		this.nodeState = new HashMap<Integer, NodeState>();
		this.buffers = buffers;
		this.selector = selector;
		this.maxBatchSize = maxBatchSize;
		this.lingerMs = lingerMs;
		this.cluster = Cluster.empty();
		this.metadata = metadata;
		this.clientId = clientId;
		this.running = true;
		this.requestTimeout = requestTimeout;
		this.acks = acks;
		this.inFlightRequests = new InFlightRequests();
	}
	
	/*
	 * partition info
	 *   - buffer for reuse
	 * TODO: Probably shouldn't throw any exceptions
	 */
	public void run() {
    List<NetworkSend> sends = new ArrayList<NetworkSend>();
		while(running) {
			// ready partitions - in flight or blacklisted partitions
			long now = System.currentTimeMillis();
			boolean fetchMetadata = false;
			List<TopicPartition> ready = this.buffers.ready(now);
			
			// prune the list of ready topics to eliminate any that we aren't ready to process yet
			Iterator<TopicPartition> iter = ready.iterator();
			while(iter.hasNext()) {
			  TopicPartition tp = iter.next();
			  Node node = cluster.leaderFor(tp);
			  if(node == null) {
			    // we don't know about this topic/partition, re-fetch metadata
			    fetchMetadata = true;
			    iter.remove();
			  } else if(nodeState.get(node) == null) {
			    // we don't have a connection to this node yet, make one
			    try {
  			    selector.connect(node.id(), new InetSocketAddress(node.host(), node.port()), 64*1024*1024, 64*1024*1024); // TODO socket buffers
	          nodeState.put(node.id(), NodeState.CONNECTING);
			    } catch(IOException e) {
	           // TODO: handle me
			      throw new RuntimeException(e);
			    }
			    iter.remove();
			  } else if(nodeState.get(node) != NodeState.CONNECTED) {
			    // we are connecting but haven't finished yet.
			    iter.remove();
			  } else if(!inFlightRequests.canSendMore(node.id())) {
			    // we haven't finished sending our existing requests
			    iter.remove();
			  }
			}
			
			// should we update our metadata?
			// TODO: do a periodic refresh just for good measure
			// TODO: don't hard-code backoff
			if(fetchMetadata == true && this.lastMetadataFetch > 100) {
			  InFlightRequest req = metadataRequest(metadata.topics());
			  sends.add(req.request);
			  this.inFlightRequests.add(req);
			}
			
			// create produce requests
			List<RecordBuffer> buffers = this.buffers.drain(ready);
			collate(cluster, buffers, sends);
			
			// do the I/O
			try {
  			this.selector.poll(1000L, sends);
			} catch(IOException e) {
			  throw new RuntimeException(e);
			}
			
			handleResponses(this.selector.completedReceives(), now);
			
			handleDisconnects(this.selector.disconnected());
		}
	}
	
	private void handleResponses(List<NetworkReceive> receives, long now) {
    for(NetworkReceive receive: receives) {
      int source = receive.source();
      InFlightRequest req = inFlightRequests.nextCompleted(source);
      ResponseHeader header = ResponseHeader.parse(receive.payload());
      short apiKey = req.request.header().apiKey();
      Struct body = (Struct) ProtoUtils.currentRequestSchema(apiKey).read(receive.payload());
      correlate(req.request.header(), header);
      if(req.request.header().apiKey() == ApiKeys.PRODUCE.id)
        handleProduceResponse(req, body);
      else if(req.request.header().apiKey() == ApiKeys.METADATA.id)
        handleMetadataResponse(body, now);
      else
        throw new IllegalStateException("Unexpected response type: " + req.request.header().apiKey());
    }
	}
	
	private void handleDisconnects(List<Integer> disconnects) {
	  for(int node: disconnects) {
  	  for(InFlightRequest request: this.inFlightRequests.clearAll(node)) {
  	    if(request.buffers != null) {
  	      for(RecordBuffer buffer: request.buffers.values())
  	        buffer.produceFuture.done(-1L, Errors.NETWORK_EXCEPTION.code());
  	    }
  	  }
	  }
	}
	
	private void handleMetadataResponse(Struct response, long now) {
	  this.lastMetadataFetch = now;
	  this.metadata.update(ProtoUtils.parseMetadataResponse(response));
	}
	
	private void handleProduceResponse(InFlightRequest request, Struct response) {
	  for(Struct topicResponse: (Struct[]) response.get("responses")) {
	    String topic = (String) topicResponse.get("topic_name");
	    for(Struct partResponse: (Struct[]) topicResponse.get("partition_response")){
	      int partition = (Integer) partResponse.get("partition");
	      short errorCode = (Short) partResponse.get("error_code");
	      long offset = (Long) partResponse.get("offset");
	      RecordBuffer buffer = request.buffers.get(new TopicPartition(topic, partition));
	      buffer.produceFuture.done(offset, errorCode);
	    }
	  }
	}
	
	private void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
    if(requestHeader.correlationId() != responseHeader.correlationId())
      throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId() + ") does not match request (" + requestHeader.correlationId() + ")");
	}
	
	private InFlightRequest metadataRequest(Set<String> topics) {
	  String[] ts = new String[topics.size()];
	  topics.toArray(ts);
	  Struct body = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.METADATA.id));
	  body.set("topics", topics);
	  int node = cluster.nextNode().id();
	  RequestSend send = new RequestSend(node, new RequestHeader(ApiKeys.METADATA.id, clientId, correlation++), body);
	  return new InFlightRequest(send, null);
	}
	
	private void collate(Cluster cluster, List<RecordBuffer> buffers, List<NetworkSend> sends) {
	  Map<Integer, List<RecordBuffer>> collated = new HashMap<Integer, List<RecordBuffer>>();
		for(RecordBuffer buffer: buffers) {
		  Node node = cluster.leaderFor(buffer.tp);
		  List<RecordBuffer> found = collated.get(node.id());
		  if(found == null) {
		    found = new ArrayList<RecordBuffer>();
		    collated.put(node.id(), found);
		  }
		  found.add(buffer); 
		}
		for(Map.Entry<Integer, List<RecordBuffer>> entry: collated.entrySet()) {
		  InFlightRequest request = produceRequest(entry.getKey(), acks, requestTimeout, entry.getValue());
		  sends.add(request.request);
		  this.inFlightRequests.add(request);
		}
	}
	
	private InFlightRequest produceRequest(int destination, short acks, int timeout, List<RecordBuffer> buffers) {
	  Map<TopicPartition, RecordBuffer> buffersByPartition = new HashMap<TopicPartition, RecordBuffer>();
	  Map<String, List<RecordBuffer>> buffersByTopic = new HashMap<String, List<RecordBuffer>>();
	  for(RecordBuffer buffer: buffers) {
	    buffersByPartition.put(buffer.tp, buffer);
	    List<RecordBuffer> found = buffersByTopic.get(buffer.tp.topic());
	    if(found == null) {
	      found = new ArrayList<RecordBuffer>();
	      buffersByTopic.put(buffer.tp.topic(), found); 
	    }
	    found.add(buffer);
	  }
	  Struct produce = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.PRODUCE.id));
	  produce.set("acks", acks);
	  produce.set("timeout", timeout);
	  List<Struct> topicDatas = new ArrayList<Struct>(buffersByTopic.size());
	  for(Map.Entry<String, List<RecordBuffer>> entry: buffersByTopic.entrySet()) {
	    Struct topicData = produce.instance("topic_data");
	    topicData.set("topic_name", entry.getKey());
	    List<Struct> partitionData = new ArrayList<Struct>();
	    for(RecordBuffer buffer: entry.getValue()) {
	      Struct part = 
	          topicData.instance("data")
	                   .set("partition", buffer.tp.partition())
	                   .set("message_set", buffer.records.buffer());
 	      partitionData.add(part);
	    }
	    topicData.set("topic_data", partitionData);
	    topicDatas.add(topicData);
	  }
	  produce.set("topic_data", topicDatas);
	  
	  RequestHeader header = new RequestHeader(ApiKeys.PRODUCE.id, clientId, correlation++);
	  RequestSend send = new RequestSend(destination, header, produce);
	  return new InFlightRequest(send, buffersByPartition);
	}
	
	void wakeup() {
		this.selector.wakeup();
	}
	
	private class InFlightRequest {
	  public Map<TopicPartition, RecordBuffer> buffers;
	  public RequestSend request;
	  
	  public InFlightRequest(RequestSend request, Map<TopicPartition, RecordBuffer> buffers) {
	    this.buffers = buffers;
	    this.request = request;
	  }
	}
	
	private class InFlightRequests {
	  private final Map<Integer, Deque<InFlightRequest>> requests = new HashMap<Integer, Deque<InFlightRequest>>();
	  
	  public void add(InFlightRequest request) {
	    Deque<InFlightRequest> reqs = this.requests.get(request.request.destination());
	    if(reqs == null) {
	      reqs = new ArrayDeque<InFlightRequest>();
	      this.requests.put(request.request.destination(), reqs);
	    }
	    reqs.addFirst(request);
	  }
	  
	  public InFlightRequest nextCompleted(int node) {
	    Deque<InFlightRequest> reqs = requests.get(node);
	    if(reqs == null || reqs.isEmpty())
	      throw new IllegalStateException("Response from server for which there are no in-flight requests.");
	    return reqs.pollLast();
	  }
	  
	  public boolean canSendMore(int node) {
	    Deque<InFlightRequest> queue = requests.get(node);
	    return queue != null && !queue.isEmpty() && queue.peekFirst().request.remaining() > 0;
	  }
	  
	  public Iterable<InFlightRequest> clearAll(int node) {
	    Deque<InFlightRequest> reqs = requests.get(node);
	    if(reqs == null) {
	      return Collections.emptyList();
	    } else {
	      return requests.remove(node);
	    }
	  }
	}
	
}
