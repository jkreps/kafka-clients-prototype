package kafka.common.requests;

import java.nio.ByteBuffer;

import kafka.clients.common.network.NetworkSend;
import kafka.common.protocol.types.Struct;

public class RequestSend extends NetworkSend {
      
  private final RequestHeader header;
  private final Struct body;
  
  public RequestSend(int destination, RequestHeader header, Struct body) {
    super(destination, serialize(header, body));
    this.header = header;
    this.body = body;
    this.buffers[0].putInt(this.buffers[0].remaining() - 4);
    this.header.writeTo(this.buffers[0]);
    this.body.writeTo(this.buffers[0]);
  }
  
  private static ByteBuffer serialize(RequestHeader header, Struct body) {
    ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
    header.writeTo(buffer);
    body.writeTo(buffer);
    buffer.rewind();
    return buffer;
  }
  
  public RequestHeader header() {
    return this.header;
  }
  
  public Struct body() {
    return body;
  }
  
}
