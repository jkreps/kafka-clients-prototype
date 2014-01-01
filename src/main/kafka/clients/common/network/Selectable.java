package kafka.clients.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public interface Selectable {

  public abstract void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException;
  public abstract void disconnect(int id);
  public abstract void wakeup();
  public abstract void close();
  public abstract void poll(long timeout, List<NetworkSend> sends) throws IOException;
  public abstract List<NetworkSend> completedSends();
  public abstract List<NetworkReceive> completedReceives();
  public abstract List<Integer> disconnected();
  public abstract List<Integer> connected();

}