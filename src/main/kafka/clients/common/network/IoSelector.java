package kafka.clients.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A selector interface for doing asynchronous I/O against multiple connections.
 * 
 * This class is not thread safe!
 */
public class IoSelector {
	
	private final java.nio.channels.Selector selector;
	private final Map<Integer, SelectionKey> keys;
	private final List<NetworkSend> completedSends;
	private final List<NetworkReceive> completedReceives;
	private final List<Integer> disconnected;
	

	public IoSelector() throws IOException {
		this.selector = java.nio.channels.Selector.open();
		this.keys = new HashMap<Integer, SelectionKey>();
		this.completedSends = new ArrayList<NetworkSend>();
		this.completedReceives = new ArrayList<NetworkReceive>();
		this.disconnected = new ArrayList<Integer>();
	}

	/**
	 * Add a new connection to this selector associated with the given id number
	 * @param id The id for the new connection
	 * @param address The address to connect to
	 * @param sendBufferSize The send buffer for the new connection
	 * @param receiveBufferSize The receive buffer for the new connection
	 */
	public void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
		Socket socket = new Socket();
		socket.setKeepAlive(true);
		socket.setSendBufferSize(sendBufferSize);
		socket.setReceiveBufferSize(receiveBufferSize);
		socket.setTcpNoDelay(true);
		SocketChannel channel = socket.getChannel();
		channel.configureBlocking(false);
		channel.connect(address);
		SelectionKey key = channel.register(this.selector, SelectionKey.OP_CONNECT);
		key.attach(new Transmissions(id));
		if(this.keys.containsKey(key))
			throw new IllegalStateException("There is already a connection for id " + id);
		this.keys.put(id, key);
	}
	
	public void wakeup() {
		this.selector.wakeup();
	}
	
	/**
	 * Close this selector and all associated connections
	 */
	public void close() throws IOException {
		for(SelectionKey key: this.selector.keys())
			key.channel().close();
		this.selector.close();
	}
	
	/**
	 * Attempt to begin sending the Sends in the send list and return any completed receives in receives list.
	 * @param timeout The amount of time to wait, in milliseconds. If negative, wait indefinitely.
	 * @param sends The list of new sends to begin
	 */
	public void poll(long timeout, List<NetworkSend> sends) throws IOException {
    clear();
		
		/* register for write interest on any new sends */
		for(NetworkSend send: sends) {
			SelectionKey key = keyForId(send.destination());
			Transmissions transmissions = transmissions(key);
			if(transmissions.hasSend())
				throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
			transmissions.send = send;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
		}
		
		/* check ready keys */
		int readyKeys = select(timeout);
		if(readyKeys > 0) {
			Set<SelectionKey> keys = this.selector.selectedKeys();
			Iterator<SelectionKey> iter = keys.iterator();
			while(iter.hasNext()) {
				SelectionKey key = iter.next();
				iter.remove();
				
				Transmissions transmissions = transmissions(key);
				SocketChannel channel = channel(key);
				
				/* complete any connections that have finished their handshake */
				if(key.isConnectable()) {
					channel.finishConnect();
					key.interestOps(key.interestOps() | SelectionKey.OP_READ);
				}
			    
				/* read from any connections that have readable data */
				if(key.isReadable()) {
					if(!transmissions.hasReceive())
						; // make a new one
					try {
						transmissions.receive.readFrom(channel);
					} catch(IOException e) {
						close(key);
					}
					if(transmissions.receive.complete()) {
						this.completedReceives.add(transmissions.receive);
						transmissions.clearReceive();
					}
				}
				
				/* write to any sockets that have space in their buffer and for which we have data */
				if(key.isWritable()) {
					if(!transmissions.hasSend())
						; // make a new one
					try {
						transmissions.send.writeTo(channel);
					} catch(IOException e) {
						close(key);
					}
					if(transmissions.send.remaining() <= 0) {
						this.completedSends.add(transmissions.send);
						transmissions.clearSend();
						key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
					}
				}
				
				/* cancel any defunct sockets */
				if(!key.isValid())
					close(key);
			}
		}
	}
	
	public List<NetworkSend> completedSends() {
	  return this.completedSends;
	}
	
	public List<NetworkReceive> completedReceives() {
	  return this.completedReceives;
	}
	
	public List<Integer> disconnected() {
	  return this.disconnected;
	}
	
	/**
	 * Clear the results from the prior poll
	 */
	private void clear() {
	  this.completedSends.clear();
	  this.completedReceives.clear();
	  this.disconnected.clear();
	}
	
	/**
	 * Check for data, waiting up to the given timeout.
	 * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
	 * @return The number of keys ready
	 * @throws IOException
	 */
	private int select(long ms) throws IOException {
		if(ms == 0L)
			return this.selector.selectNow();
		else if(ms < 0L)
			return this.selector.select();
		else
			return this.selector.select(ms);
	}
	
	private void close(SelectionKey key) throws IOException {
		SocketChannel channel = channel(key);
		this.disconnected.add(transmissions(key).id);
		key.attach(null);
		key.cancel();
		channel.socket().close();
		channel.close();
	}
	
	private SelectionKey keyForId(int id) {
		SelectionKey key = this.keys.get(id);
		if(key == null)
			throw new IllegalStateException("Attempt to write to socket for which there is no open connection.");
		return key;
	}
	
	private Transmissions transmissions(SelectionKey key) {
		return (Transmissions) key.attachment();
	}
	
	private SocketChannel channel(SelectionKey key) {
		return (SocketChannel) key.channel();
	}
	
	private static class Transmissions {
	  public int id;
		public NetworkSend send;
		public NetworkReceive receive;
		
		public Transmissions(int id) {
		  this.id = id;
		}
		
		public boolean hasSend() {
			return this.send != null;
		}
		
		public void clearSend() {
			this.send = null;
		}
		
		public boolean hasReceive() {
			return this.receive != null;
		}
		
		public void clearReceive() {
			this.receive = null;
		}
	}
	
}
