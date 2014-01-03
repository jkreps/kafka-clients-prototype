package kafka.clients.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public interface Send {

    public int destination();

    public int remaining();

    public boolean complete();

    /**
     * An optional method to turn this send into an array of ByteBuffers if possible (otherwise returns null)
     */
    public ByteBuffer[] reify();

    public long writeTo(GatheringByteChannel channel) throws IOException;

}
