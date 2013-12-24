package kafka.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import kafka.clients.common.network.NetworkReceive;
import kafka.clients.common.network.NetworkSend;
import kafka.clients.common.network.Selectable;

public class MockSelector implements Selectable {
  
  @Override
  public void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
    
  }
  
  @Override
  public void disconnect(int id) {
    
  }
  
  @Override
  public void wakeup() {
    
  }
  
  @Override
  public void close() {
    
  }
  
  @Override
  public void poll(long timeout, List<NetworkSend> sends) throws IOException {
    
  }

  @Override
  public List<NetworkSend> completedSends() {
    return null;
  }

  @Override
  public List<NetworkReceive> completedReceives() {
    return null;
  }

  @Override
  public List<Integer> disconnected() {
    return null;
  }

  @Override
  public List<Integer> connected() {
    return null;
  }

}
