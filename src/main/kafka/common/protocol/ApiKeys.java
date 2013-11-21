package kafka.common.protocol;

/**
 * The api keys for the kafka protocol.
 * 
 * DO NOT MODIFY OR REUSE EXISTING VALUES.
 */
public class ApiKeys {
	public static final short PRODUCER_REQUEST = 0;
	public static final short FETCH_REQUEST = 1;
	public static final short LIST_OFFSETS_REQUEST = 2;
	public static final short METADATA_REQUEST = 3;
	public static final short LEADER_AND_ISR_REQUEST = 4;
	public static final short STOP_REPLICA_REQUEST = 5;
	public static final short OFFSET_COMMIT_REQUEST = 6;
	public static final short OFFSET_FETCH_REQUEST = 7;
}
