package com.feeyo.raft.storage.snapshot;

/**
 * 磁盘读/写期间的快照限制
 */
public interface SnapshotThrottle {
	//
    // 限制后的可用吞吐量
    long throttledByThroughput(final long bytes);
}
