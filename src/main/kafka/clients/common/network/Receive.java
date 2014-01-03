package kafka.clients.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

public interface Receive {

    public int source();

    public boolean complete();

    public ByteBuffer[] reify();

    public int readFrom(ScatteringByteChannel channel) throws IOException;

}
