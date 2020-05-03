package com.feeyo.raft.storage.snapshot;

/**
 * Snapshot throttling during heavy disk reading/writing
 *
 * @author dennis
 */
public interface SnapshotThrottle {

    /**
     * Get available throughput in bytes after throttled
     * Must be thread-safe
     *
     * @param bytes expect size
     * @return available size
     */
    long throttledByThroughput(final long bytes);
}
