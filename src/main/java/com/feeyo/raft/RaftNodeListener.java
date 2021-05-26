package com.feeyo.raft;

import com.feeyo.raft.proto.Raftpb.Message;

/** 
 * @see https://github.com/etcd-io/etcd/blob/main/raft/node.go
 * 
 * @author zhuam
 */
public interface RaftNodeListener {
	
	void onStateChange(long id, StateType newStateType, long leaderId);  // raft 状态变更
	void onReadIndex(String rctx, long index); 	// StepFollower(MsgReadIndex) & StepLeader(MsgReadIndexResp/MsgHeartbeatResponse) 触发
	void onAppliedIndex(long appliedIndex); // applied 索引更新
	//
	void onReceivedHeartbeat(Message msg);	// follower & candidate 接收了 leader 的 MsgHeartbeat
	void onProposalForwarding(Message msg); // follower 收到客户端 MsgPropose 需要转发
	void onSendSnapshots(Message msg); // leader 发送快照 MsgSnapshot
	//
	boolean isAllowLaunchElection(); // 是否允许通过比较节点的优先级和目标优先级来启动选举
}
