package com.feeyo.raft.storage;

import java.util.List;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Pair;

//
public abstract class Storage {
	
	// HardState 保存着该 Raft 节点最后一次保存的 term 信息，之前 vote 哪一个节点，以及已经 commit 的 log index
	protected HardState hardState;

	public synchronized void setHardState(HardState hardState) {
		if ( hardState != null )
			this.hardState = hardState;
	}

	// 返回保存的初始状态，包括硬状态、配置状态( 存储了集群中有哪些节点 )
	public abstract Pair<HardState, ConfState> initialState();

	// 传入一个索引值，返回这个索引值对应的任期号，如果不存在则抛出异常，
	// 		ErrCompacted：表示传入的索引数据已经找不到，说明已经被压缩成快照数据了。
	// 		ErrUnavailable：表示传入的索引值大于当前的最大索引
	public abstract long getTerm(long index) throws RaftException;

	// 返回第一条数据的索引值
	public abstract long firstIndex(); 

	// 获得最后一条数据的索引值
	public abstract long lastIndex(); 

	// 返回索引范围在 [low, high)之内并且不大于 maxSize 的 entries 数组
	public abstract List<Entry> getEntries(long low, long high, long maxSize) throws RaftException;
	
	// 返回最新快照的元数据
	public abstract SnapshotMetadata getSnapshotMetadata();

	// 应用快照的元数据
	public abstract void applySnapshotMetadata(SnapshotMetadata meta) throws RaftException;

	// 添加数据
	public abstract void append(List<Entry> entries) throws RaftException;

	// 数据压缩，将compactIndex之前的数据丢弃掉
	public abstract void compact(long compactIndex) throws RaftException;

	// 根据传入的数据创建快照
	public abstract Snapshot createSnapshot(long appliedIndex, ConfState cs, byte[] data, long seqNo, boolean last)
			throws RaftException;
	
	//
	public abstract void close();

}
