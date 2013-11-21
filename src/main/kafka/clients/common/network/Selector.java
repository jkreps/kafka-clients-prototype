package kafka.clients.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
public class Selector {
	
	private final java.nio.channels.Selector selector;
	private final Map<Integer, SelectionKey> keys;
	

	public Selector() throws IOException {
		this.selector = java.nio.channels.Selector.open();
		this.keys = new HashMap<Integer, SelectionKey>();
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
		key.attach(new Transmissions());
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
	 * @param completed An empty list which this method will populate with completed sends to the remote server
	 * @param receives An empty list which this method will populate with completed receives to the remove server
	 * @param disconnects The ids for nodes which disconnected. When filled in by the client this method will initiate a disconnect; when
	 * the remote node disconnects this method will fill in the value.
	 */
	public void poll(long timeout, 
				     List<Send> sends, 
					 List<Send> completed, 
					 List<Receive> receives,
					 List<Integer> disconnects) throws IOException {
		if(receives.size() > 0 || completed.size() > 0)
			throw new IllegalArgumentException("Expected receives and completed list to be empty.");
		
		/* register for write interest on any new sends */
		for(Send send: sends) {
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
						receives.add(transmissions.receive);
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
					if(transmissions.send.complete()) {
						completed.add(transmissions.send);
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
		public Send send;
		public Receive receive;
		
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
