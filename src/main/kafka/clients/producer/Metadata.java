package kafka.clients.producer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.common.Cluster;
import kafka.common.PartitionInfo;

/**
 * A class encapsulating some of the logic around metadata.
 * 
 * Metadata is maintained for only a subset of topics, which can be added to over time.
 */
public final class Metadata {

    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private long lastRefresh;
    private Cluster cluster;
    private boolean forceUpdate;
    private final Set<String> topics;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    /**
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefresh = 0L;
        this.cluster = Cluster.empty();
        this.forceUpdate = false;
        this.topics = new HashSet<String>();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Fetch cluster metadata including partitions for the given topic. If there is no metadata for the given topic,
     * block waiting for an update.
     * 
     * @param topic The topic we want metadata for
     */
    public synchronized Cluster fetch(String topic) {
        List<PartitionInfo> partitions = null;
        do {
            partitions = cluster.partitionsFor(topic);
            if (partitions == null) {
                topics.add(topic);
                forceUpdate = true;
                try {
                    wait();
                } catch (InterruptedException e) { /* this is fine, just try again */
                }
            } else {
                return cluster;
            }
        } while (true);
    }

    /**
     * Does the current cluster info need to be updated? An update is needed if it has been at least refreshBackoffMs
     * since our last update and either (1) an update has been requested or (2) the current metadata has expired (more
     * than metadataExpireMs has passed since the last refresh)
     */
    public synchronized boolean needsUpdate(long now) {
        long msSinceLastUpdate = now - this.lastRefresh;
        boolean updateAllowed = msSinceLastUpdate >= this.refreshBackoffMs;
        boolean updateNeeded = this.forceUpdate || msSinceLastUpdate >= this.metadataExpireMs;
        return updateAllowed && updateNeeded;
    }

    /**
     * Force an update of the current cluster info
     */
    public synchronized void forceUpdate() {
        this.forceUpdate = true;
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<String>(this.topics);
    }

    /**
     * Update the cluster metadata
     */
    public synchronized void update(Cluster cluster, long now) {
        this.forceUpdate = false;
        this.lastRefresh = now;
        this.cluster = cluster;
        notifyAll();
    }

}
