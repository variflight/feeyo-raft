package com.feeyo.raft.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.feeyo.raft.Const;
import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotChunk;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Util;
import com.feeyo.raft.util.Pair;
import com.google.protobuf.ZeroByteStringHelper;

// @see https://github.com/coreos/etcd/blob/master/raft/storage.go
//
public class MemoryStorage extends Storage {
	//
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock writeLock = readWriteLock.writeLock();
	private final Lock readLock = readWriteLock.readLock();
	
	// 存储快照的元数据
	private SnapshotMetadata snapshotMeta;
	
	// 存储紧跟着快照数据的日志 entry, entries[i]保存的日志数据索引位置为 i + snapshot.Metadata.Index
	private List<Entry> entries = new ArrayList<Entry>();
	
	public MemoryStorage() {
		hardState = HardState.getDefaultInstance();
		snapshotMeta = SnapshotMetadata.getDefaultInstance();
		
		// 从头开始时，使用 term 为 zero 的虚拟条目填充列表
		Entry dummyEntry = Entry.getDefaultInstance();
		entries.add(dummyEntry);
	}
	
	// `initial_state` is called when Raft is initialized. 
	//  This interface will return a `Pair` which contains `HardState` and `ConfState`;
	@Override
	public Pair<HardState, ConfState> initialState() {	
		Pair<HardState, ConfState> pair = Pair.create(hardState, snapshotMeta.getConfState());
		return pair;
	}

	// 返回索引范围在 [low, high)之内并且不大于 maxSize 的 entries 数组
	@Override
	public List<Entry> getEntries(long low, long high, long maxSize) throws RaftException {
		readLock.lock();
        try {
        	if ( entries.isEmpty() )
				return null;
			
			long offset = entries.get(0).getIndex();
			if ( low <= offset ) 
				throw new Errors.RaftException(Errors.ErrCompacted);
			
			if (high > lastIndex() + 1) 
				throw new Errors.RaftException("entries' high(" + high + ") is out of bound lastindex(" + lastIndex() + ")");
			
			// only contains dummy entries.
			if ( entries.size() == 1 ) 
				throw new Errors.RaftException(Errors.ErrUnavailable);
			
			//
			int start = (int) (low - offset);
			int end   = (int) (high - offset);
			
			List<Entry> ents = Util.slice(entries, start, end);
			return Util.limitSize(ents, maxSize);
			
        } finally {
        	readLock.unlock();
        }
	}

	// 传入一个索引值，返回这个索引值对应的任期号，如果不存在则抛出异常，
	// 		ErrCompacted：表示传入的索引数据已经找不到，说明已经被压缩成快照数据了。
	// 		ErrUnavailable：表示传入的索引值大于当前的最大索引
	@Override
	public long getTerm(long index) throws RaftException {
		readLock.lock();
        try {
			if ( entries.isEmpty() )  {
				return Const.ZERO_TERM;
			}
			
			// 如果比当前数据最小的索引还小，说明已经被compact过了
			long offset = entries.get(0).getIndex();
			if ( index < offset ) 
				throw new Errors.RaftException(Errors.ErrCompacted);
			
			// 如果超过当前数组大小，返回不可用
			if (index - offset >= entries.size() ) 
				throw new Errors.RaftException(Errors.ErrUnavailable);
			
			Entry entry = entries.get( (int)(index - offset) );
			return entry.getTerm();
			
        } finally {
        	readLock.unlock();
        }
	}

	// 返回可以通过条目获得的第一个日志条目的索引（较旧的条目已被合并到最新的快照中；如果存储只包含虚拟条目，则第一个日志条目不可用）
	@Override
	public long firstIndex() {
		readLock.lock();
        try {
        	if ( entries.isEmpty() )
        		return Const.ZERO_IDX;
        	//
        	return entries.get(0).getIndex() + 1;
        } finally {
        	readLock.unlock();
        }
	}

	// 返回日志中最后一个条目的 index
	@Override
	public long lastIndex() {
		readLock.lock();
        try {
        	return entries.get(0).getIndex() + entries.size() - 1;
        } finally {
        	readLock.unlock();
        }
	}
	
	// 使用快照指针进行数据还原
	@Override
	public void applySnapshotMetadata(SnapshotMetadata meta) throws RaftException {
		
		if ( meta == null )
			return;
		
		writeLock.lock();
        try {
			// handle check for old snapshot being applied
			long msIndex = this.snapshotMeta.getIndex();
			long snapIndex = meta.getIndex();
			if( msIndex >= snapIndex ) {
				// 索引过期
				throw new Errors.RaftException(Errors.ErrSnapOutOfDate);
			}
				
			this.snapshotMeta = meta;
	
			// 这里也插入了一条空数据
			Entry nullEntry = Entry.newBuilder()
					.setTerm( snapshotMeta.getTerm() )
					.setIndex( snapshotMeta.getIndex() )
					.build();
			
			this.entries = new ArrayList<Entry>();
			this.entries.add( nullEntry ); 
			
        } finally {
        	writeLock.unlock();
        }
	}

	
	// 返回最新的快照
	// 如果快照暂时不可用, 抛出 SnapshotTemporarilyUnavailable 异常, 这个 Raft状态机就可以知道存储需要一些时间来准备快照并稍后调用快照
	@Override
	public SnapshotMetadata getSnapshotMetadata() {
		readLock.lock();
        try {
        	return this.snapshotMeta;
        } finally {
        	readLock.unlock();
        }
	}

	// 添加新的 entries 数据
	@Override
	public void append(List<Entry> ents) throws RaftException {
		
		if( Util.isEmpty( ents ) )
			return;
		
		writeLock.lock();
        try {
        	
			// 得到当前第一条数据的索引
			long first = this.entries.get(0).getIndex() + 1;
			//
			// 得到传入的最后一条数据的索引
			long last = ents.get(0).getIndex() + ents.size() - 1;
	
			// 检查合法性
			if ( last < first ) {
				return;
			}
			
			// truncate compacted entries
			//
			// 如果当前已经包含传入数据中的一部分，那么已经有的那部分数据可以不用重复添加进来
			if (first > ents.get(0).getIndex()) {
				int start = (int) (first - ents.get(0).getIndex());
				ents = Util.slice(ents, start, ents.size());
			}
			
			// 计算传入数据到当前已经保留数据的偏移量
			long offset = ents.get(0).getIndex() - this.entries.get(0).getIndex();
			if (this.entries.size() > offset) {
				// 如果当前数据量大于偏移量，说明offset之前的数据从原有数据中取得，之后的数据从传入的数据中取得
				this.entries = Util.slice(this.entries, 0, (int)offset);
				this.entries.addAll(ents);
	
			} else if (this.entries.size() == offset) {
				// offset刚好等于当前数据量，说明传入的数据刚好紧挨着当前的数据，所以直接添加进来就好了
				this.entries.addAll(ents);
	
			} else {
				throw new Errors.RaftException("missing log entry [last: " + lastIndex() + ", append at: " + ents.get(0).getIndex() + "]");
			}
			
        } finally {
        	writeLock.unlock();
        }
	}

	// 数据压缩，将compactIndex之前的数据丢弃掉
	@Override
	public void compact(long compactIndex) throws RaftException {
		
		writeLock.lock();
		try {
			//
			long offset = entries.get(0).getIndex();
			
			// 小于当前索引，说明已经被压缩过了
			if (compactIndex <= offset)
				throw new Errors.RaftException(Errors.ErrCompacted);
	
			// 大于当前最大索引，panic
			long lastIndex = lastIndex();
			if (compactIndex > lastIndex)
				throw new Errors.RaftException("compact " + compactIndex + " is out of bound lastindex(" + lastIndex + ")");
	
			int i = (int) (compactIndex - offset);
			//
			// @see https://github.com/lichuang/etcd-3.1.10-codedump/blob/master/raft/storage.go
			
			// 这里也是先写一个空数据
			Entry nullEntry = Entry.newBuilder()
					.setTerm( entries.get(i).getTerm() )
					.setIndex( entries.get(i).getIndex() )
					.build();
			//
			List<Entry> newEntries = new ArrayList<>();
			newEntries.add( nullEntry );
			
			// 然后再append进来
			for(int idx = i+1; idx < entries.size(); idx++) {
				newEntries.add( entries.get( idx ) );
			}
			entries = newEntries;
			
			// entries = Util.slice(entries, i, entries.size());
			
		} finally {
			writeLock.unlock();
		}
	}

	// 根据传入的数据创建快照并且返回
	@Override
	public Snapshot createSnapshot(long appliedIndex, ConfState cs, byte[] data, long seqNo, boolean last) throws RaftException {

		readLock.lock();
		try {
			
			// 检查传入的索引如果比已经有的快照索引更小，说明已经过时了
			if( appliedIndex < snapshotMeta.getIndex() )
				throw new Errors.RaftException(Errors.ErrSnapOutOfDate);
	
			// 传入的索引不能超过最大索引
			if( appliedIndex > lastIndex() ) 
				throw new Errors.RaftException("snapshot " + appliedIndex + " is out of bound lastindex(" + lastIndex() + ")");
			
			long index = this.entries.get(0).getIndex();
			long term = this.entries.get( (int) (appliedIndex - index) ).getTerm();
			
			//
			//  Snapshot is built here for it.
			//
			// Metadata
			SnapshotMetadata.Builder metadataBuilder = SnapshotMetadata.newBuilder();
			metadataBuilder.setIndex(appliedIndex);
			metadataBuilder.setTerm(term);
			if( cs != null ) 
				metadataBuilder.setConfState(cs);
			SnapshotMetadata metadata = metadataBuilder.build();
			
			// Chunk
			SnapshotChunk.Builder chunkBuilder = SnapshotChunk.newBuilder();
			chunkBuilder.setSeqNo( seqNo );
			chunkBuilder.setLast( last );
			if( data != null)
				chunkBuilder.setData( ZeroByteStringHelper.wrap(data) );
			SnapshotChunk chunk = chunkBuilder.build();
			//
			Snapshot snapshot =  Snapshot.newBuilder()		//
					.setMetadata( metadata )	//
					.setChunk( chunk )	//
					.build();
			
			// 更新 Snapshot 元数据
			this.snapshotMeta = metadata;
			//
			// Snapshot
			return  snapshot;
		} finally {
			readLock.unlock();
		}
	}
	
	@Override
	public void close() {
		// ignored
	}
}