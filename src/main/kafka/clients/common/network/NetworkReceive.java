package kafka.clients.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

public class NetworkReceive implements Receive {
  
  private final int source;
  private final ByteBuffer size; 
  private ByteBuffer buffer;
  
  public NetworkReceive(int source) {
    this.source = source;
    this.size = ByteBuffer.allocate(4);
    this.buffer = null;
  }

  @Override
  public int source() {
    return source;
  }

  @Override
  public boolean complete() {
    return !size.hasRemaining() && !buffer.hasRemaining();
  }

  @Override
  public ByteBuffer[] reify() {
    return new ByteBuffer[]{this.buffer};
  }

  @Override
  public int readFrom(ScatteringByteChannel channel) throws IOException {
    int read = 0;
    if(size.hasRemaining()) {
      read += channel.read(size);
      if(!size.hasRemaining()) {
        size.rewind();
        int requestSize = size.getInt();
        if(requestSize < 0)
          throw new IllegalStateException("Invalid request (size = " + requestSize + ")");
        this.buffer = ByteBuffer.allocate(requestSize);
      }
    }
    if(buffer != null)
      read += channel.read(buffer);

    return read;
  }
  
  public ByteBuffer payload() {
    return this.buffer;
  }
  
}
