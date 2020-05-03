package com.feeyo.raft.group;

import com.feeyo.raft.PeerSet;
import com.feeyo.raft.StateMachine.SnapshotHandler;
import com.feeyo.raft.Errors.RaftException;

/**
 * Multi-Raft 状态机
 * 
 * @author zhuam
 *
 */
public interface RaftGroupStateMachine {
	//
	//
	void initialize(final long regionId, long committed);
	//
	// entries & member 
	boolean apply(final long regionId, byte[] data, long committed);
	void applyMemberChange(final long regionId, PeerSet peerSet, long committed);
	
	//
	// snapshot
	void applySnapshot(final long regionId, boolean toCleanup, byte[] data);
	void takeSnapshot(final long regionId, final SnapshotHandler handler) throws RaftException;
	
	//
	void leaderChange(final long regionId, long leaderId);
	void leaderCommitted(final long regionId, long leaderCommitted);
	//
}