package kafka.common.protocol;

public class ErrorCodes {
	
	/**
	 * Something bad happened for which we don't have a standardized error code.
	 */
	public static final short UNKNOWN_ERROR = -1;
	
	/**
	 * It worked!
	 */
	public static final short NO_ERROR = 0;
	
	/**
	 * The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
	 */
	public static final short OFFSET_OUT_OF_RANGE =	1;
	
	/**
	 * This indicates that a message contents does not match its CRC.
	 */
	public static final short INVALID_MESSAGE = 2;
	
	/**
	 * This request is for a topic or partition that does not exist on this broker.
	 */
	public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
	
	/**
	 * The message has a negative or otherwise impossible size.
	 */
	public static final short INVALID_MESSAGE_SIZE = 4;
	
	/**
	 * This error is thrown if we are in the middle of a leadership election and there is 
	 * currently no leader for this partition and hence it is unavailable for writes.
	 */
	public static final short LEADER_NOT_AVAILABLE = 5; 
	
	/**
	 * This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition.
	 * It indicates that the clients metadata is out of date.
	 */
	public static final short NOT_LEADER_FOR_PARTITION	= 6;
	
	/**
	 * This error is thrown if the request exceeds the user-specified time limit in the request.
	 */
	public static final short REQUEST_TIMED_OUT = 7;
	
	/**
	 * This is not a client facing error and is used only internally by intra-cluster broker communication.
	 */
	public static final short BROKER_NOT_AVAILABLE = 8;
	
	public static final short REPLICA_NOT_AVAILABLE = 9;
	
	/**
	 * The server has a configurable maximum message size to avoid unbounded memory allocation. 
	 * This error is thrown if the client attempt to produce a message larger than this maximum.
	 */
	public static final short MESSAGE_SIZE_TOO_LARGE = 10;
	
	public static final short STALE_CONTROLLOR_EPOCH = 11;
	
	/**
	 * If you specify a string larger than configured maximum for offset metadata
	 */
	public static final short OFFSET_METADATA_TOO_LARGE =  12;
	
	/**
	 * An unknown network error
	 */
	public static final short SERVER_DISCONNECTED = 13;
}
