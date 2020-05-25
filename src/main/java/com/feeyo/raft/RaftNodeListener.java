package com.feeyo.raft;

import com.feeyo.raft.proto.Raftpb.Message;

/**
 * 
 * @author zhuam
 *
 */
public interface RaftNodeListener {
	//
	void onStateChange(long id, StateType newStateType, long leaderId); // State change
	//
	void onReadIndex(String rctx, long index); 		// 线性一致性读
	void onAppliedIndex(long appliedIndex); 		// 应用索引
	//
	void onReceivedHeartbeat(Message msg);		 	// Leader MsgHeartbeat
	void onProposalForwarding(Message msg); 		// Follower 收到 MsgPropose 需要转发
	void onSendSnapshots(Message msg);			 	// Leader 给 Follower 发送快照 MsgSnapshot
	//
	Peer getPeer();
	PeerSet getPeerSet();
	//
	boolean isAllowElection();						// 是否允许通过比较节点的优先级和目标优先级来启动选举
	int getNodeTargetPriority();					// 获取节点的目标选择优先级值
	int getMaxPriorityOfNodes();					// 获取同一Raft组中所有节点的最大优先级值，并更新当前节点的目标优先级值
}
