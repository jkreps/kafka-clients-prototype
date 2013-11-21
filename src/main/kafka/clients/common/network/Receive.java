package kafka.clients.common.network;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

public interface Receive {
	
	public int source();

	public boolean complete();
	
	public int readFrom(ScatteringByteChannel channel) throws IOException;
	
}
