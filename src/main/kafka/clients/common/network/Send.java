package kafka.clients.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public interface Send {
	
	public int destination();

	public boolean complete();
	
	public int writeTo(GatheringByteChannel channel) throws IOException;
	
}
