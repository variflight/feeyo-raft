package com.feeyo.raft;

/**
 * @see https://github.com/etcd-io/etcd/blob/main/raft/tracker/state.go
 * @see https://github.com/tikv/raft-rs/blob/master/src/tracker/state.rs
 * 
 *  Probe：leader每个心跳间隔最多发送一条复制消息该节点。 它还探讨了follower的实际进展
 *  Replicate：正常的接收数据状态，leader在发送复制消息后，修改该节点的next索引为发送消息的最大索引+1，(TODO:这是一种优化状态，用于快速将日志条目复制到follower)
 *  Snapshot：接收快照状态, leader应该先发送快照并停止发送任何复制消息
 */
public enum ProgressState {
    Probe, 
    Replicate, 
    Snapshot; 
    
    public static String toString(ProgressState state) {
    	if ( state == Probe)
    		return "Probe";
    	else if ( state == Replicate ) 
    		return "Replicate";
    	else if ( state == Snapshot )
    		return "Snapshot";
    	return "Unknow";
    }
}
