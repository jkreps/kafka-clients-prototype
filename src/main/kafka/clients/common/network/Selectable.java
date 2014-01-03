package kafka.clients.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public interface Selectable {

    public void connect(int id,
                        InetSocketAddress address,
                        int sendBufferSize,
                        int receiveBufferSize) throws IOException;

    public void disconnect(int id);

    public void wakeup();

    public void close();

    public void poll(long timeout,
                     List<NetworkSend> sends) throws IOException;

    public List<NetworkSend> completedSends();

    public List<NetworkReceive> completedReceives();

    public List<Integer> disconnected();

    public List<Integer> connected();

}