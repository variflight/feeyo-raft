package com.feeyo.raft;

import com.feeyo.raft.proto.Raftpb.Message;

/**
 * 
 * @author zhuam
 *
 */
public interface RaftListener {

	/*
	 * State change
	 */
	 void onStateChange(long id, StateType newStateType, long leaderId);
	
	/*
	 * Linearizable read
	 */
	 void onReadIndex(String rctx, long index);
	
	/*
	 * index of applied 
	 */
	void onAppliedIndex(long appliedIndex);
	
	/*
	 * MsgHeartbeat from Leader
	 */
	 void onReceivedHeartbeat(Message msg);
	
	/*
	 * MsgPropose on follower need to be forwarded
	 */
	 void onProposalForwarding(Message msg);
	
	
	/*
	 * MsgSnapshot, send a snapshot to the follower node
	 */
	 void onSendSnapshots(Message msg);
	
}
