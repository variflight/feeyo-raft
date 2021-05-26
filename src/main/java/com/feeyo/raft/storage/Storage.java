package com.feeyo.raft.storage;

import java.util.List;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Pair;

/**
 * 
 * @see https://github.com/etcd-io/etcd/blob/main/raft/storage.go
 * @see https://github.com/lichuang/etcd-3.1.10-codedump/blob/master/raft/storage.go
 * 
 * @author xuwenfeng
 * @author zhuam
 *
 */
public abstract class Storage {
	//
	protected HardState hardState; // Raft节点最后一次保存的term，之前vote，以及已经commit的index

	public synchronized void setHardState(HardState hardState) {
		if (hardState != null)
			this.hardState = hardState;
	}
	//
	public abstract Pair<HardState, ConfState> initialState(); // 返回保存的初始状态
	//
	public abstract long getTerm(long index) throws RaftException; // 传入一个索引值，返回这个索引值对应的任期号，如果不存在则抛出异常
	public abstract long firstIndex(); // 返回第一条数据的索引值
	public abstract long lastIndex(); // 获得最后一条数据的索引值
	public abstract List<Entry> getEntries(long low, long high, long maxSize) throws RaftException; // 返回索引范围在[low,	high)之内, 小于maxSize的entries
	//
	public abstract void append(List<Entry> entries) throws RaftException; // 添加数据
	public abstract void compact(long compactIndex) throws RaftException; // 压缩数据，将compactIndex之前的数据丢弃掉
	
	public abstract SnapshotMetadata getSnapshotMetadata(); // 返回最新快照metadata
	public abstract void applySnapshotMetadata(SnapshotMetadata metadata) throws RaftException; // 使用快照metadata进行数据还原
	public abstract Snapshot createSnapshot(long appliedIndex, ConfState cs, byte[] data, long seqNo, boolean last)
			throws RaftException; // 创建快照数据

	public abstract void close();
}
