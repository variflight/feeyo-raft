package com.feeyo.raft.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.Const;
import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotChunk;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Pair;
import com.feeyo.raft.util.ProtobufCodedOutputUtil;
import com.feeyo.raft.util.Util;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * 基于 FileBuffer 实现
 * 
 * @author zhuam
 */
public class FileStorage extends Storage {

	//
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock writeLock = readWriteLock.writeLock();
	private final Lock readLock = readWriteLock.readLock();
	//
	// Saved metadata for the snapshot
	private SnapshotMetadata snapMetadata;
	
	//
	// 存储紧跟着快照数据的日志 entry, entries[i] 保存的日志数据索引位置为 i + snapshot.Metadata.Index
	private volatile List<EntryLocation> entries = new ArrayList<>();
	private FileBuffer fileBuffer = null;
	
	//
	// Entry Location & term/index
	private static class EntryLocation {
		//
		private long term;
		private long index;
		//
		// Saved the location of the entry in the file buffer
		private long position;
		private int length;
		
		public EntryLocation(long term, long index, long position, int length) {
			this.term = term;
			this.index = index;
			//
			this.position = position;
			this.length = length;
		}
		
		public long getTerm() {
			return term;
		}
		
		public long getIndex() {
			return index;
		}
		
		public long getPosition() {
			return position;
		}
		
		public int getLength() {
			return length;
		}
		
		@Override
		public String toString() {
			StringBuffer strSb = new StringBuffer(60);
			strSb.append("term=").append( term ).append(", ");
			strSb.append("index=").append( index ).append(", ");
			strSb.append("position=").append( position ).append(", ");
			strSb.append("length=").append( length );
			return  strSb.toString();
		}
	}
	
	//
	private List<Entry> getEntriesByLocations(List<EntryLocation> locations, long maxSize) throws RaftException {
		//
		int size = 0;
		//
		List<Entry> ents = new ArrayList<>();
		//
		int locationSz = locations.size();
		for(int i = 0; i < locationSz; i++) {
			//
			EntryLocation location = locations.get(i);
			try {
				//
				Entry entry = null;
				if ( location.getLength() != 0 ) {
					//
					long position = location.getPosition() - location.getLength();
					int length = location.getLength();
					//
					// byte[] data = fileBuffer.readByteArray(position, length);
					ByteBuffer data = fileBuffer.readByteBuffer(position, length);	
				 	entry = Entry.parseFrom( data );
				 	//
				} else {
					entry = Entry.newBuilder() //
							.setTerm( location.getTerm() ) //
							.setIndex( location.getIndex() ) //
							.build(); //
				}
				ents.add( entry );
				//
				// limit size
				size += entry.getSerializedSize();
				if (size > maxSize )
					break;
				//
			} catch (Throwable e1) {
				throw new RaftException("getEntriesByLocations err: " + location, e1);
			}
		}
		return ents;
	}
	
	public FileStorage(String storageDir) {
		//
		String baseDir = storageDir + File.separator + "buf";
		this.fileBuffer = new FileBuffer( baseDir );
		//
		this.hardState = HardState.getDefaultInstance();
		this.snapMetadata = SnapshotMetadata.getDefaultInstance();
		
		// 从头开始时，使用 term 为 zero 的虚拟条目填充列表
		EntryLocation dummyEntryLocation = new EntryLocation(0L, 0L, 0, 0);
		this.entries.add( dummyEntryLocation );
	}
	
	//
	// `initial_state` is called when Raft is initialized. 
	//  This interface will return a `Pair` which contains `HardState` and `ConfState`;
	@Override
	public Pair<HardState, ConfState> initialState() {	
		Pair<HardState, ConfState> pair = Pair.create(hardState, snapMetadata.getConfState());
		return pair;
	}

	// 返回索引范围在 [low, high)之内并且不大于 maxSize 的 entries 数组
	@Override
	public List<Entry> getEntries(long low, long high, long maxSize) throws RaftException {
		//
		List<EntryLocation> locations = null;
		//
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
			
			//
			locations = Util.slice(entries, start, end);
			List<Entry> ents = getEntriesByLocations(locations, maxSize );
			//return Util.limitSize(ents, maxSize);
			return ents;
			
        } finally {
        	//
        	if ( locations != null )
        		locations.clear();
        	//
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
			if ( entries.isEmpty() )  
				return Const.ZERO_TERM;
			//
			// 如果比当前数据最小的索引还小，说明已经被compact过了
			long offset = entries.get(0).getIndex();
			if ( index < offset ) 
				throw new Errors.RaftException(Errors.ErrCompacted);
			//
			// 如果超过当前数组大小，返回不可用
			if (index - offset >= entries.size() ) 
				throw new Errors.RaftException(Errors.ErrUnavailable);
			//
			EntryLocation entry = entries.get( (int)(index - offset) );
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
		//
		if ( meta == null )
			return;
		
		writeLock.lock();
        try {
        	long snapTerm = meta.getTerm();
        	long snapIndex = meta.getIndex();
        	//
			// handle check for old snapshot being applied
			long msIndex = this.snapMetadata.getIndex();
			if( msIndex >= snapIndex ) 
				throw new Errors.RaftException(Errors.ErrSnapOutOfDate); // 索引过期
			//	
			this.snapMetadata = meta;
			//
			List<EntryLocation> oldEntries = this.entries;
			List<EntryLocation> newEntries = new ArrayList<>( oldEntries.size() );
			newEntries.add(new EntryLocation(snapTerm, snapIndex, 0, 0) ); // 这里也插入了一条空数据
			this.entries = newEntries;
			//
			// clear to let GC do its work
			if ( oldEntries != null ) {
				oldEntries.clear();
				oldEntries = null;
			}
			
			//
			this.fileBuffer.clear();
			
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
        	return this.snapMetadata;
        } finally {
        	readLock.unlock();
        }
	}

	// 添加新的 entries 数据
	@Override
	public void append(List<Entry> ents) throws RaftException {
		//
		if( Util.isEmpty( ents ) )
			return;
		//
		writeLock.lock();
        try {
        	//
			long first = this.entries.get(0).getIndex() + 1;		// 得到当前第一条数据的索引
			long last = ents.get(0).getIndex() + ents.size() - 1; 	// 得到传入的最后一条数据的索引
			//
			// 检查合法性
			if ( last < first ) 
				return;
			//
			// truncate compacted entries
			//
			// 如果当前已经包含传入数据中的一部分，那么已经有的那部分数据可以不用重复添加进来
			if (first > ents.get(0).getIndex()) {
				int start = (int) (first - ents.get(0).getIndex());
				ents = Util.slice(ents, start, ents.size());
			}
			//
			// 计算传入数据到当前已经保留数据的偏移量
			long offset = ents.get(0).getIndex() - this.entries.get(0).getIndex();
			if (this.entries.size() > offset) {
				//
				// 如果当前数据量大于偏移量，说明offset之前的数据从原有数据中取得，之后的数据从传入的数据中取得
				List<EntryLocation> oldEntries = this.entries;
				List<EntryLocation> newEntries = new ArrayList<EntryLocation>();
				for (int i = 0; i < offset; i++)
					newEntries.add( oldEntries.get(i) );
				//
				this.entries = newEntries;
				//
				// clear to let GC do its work
				if ( oldEntries != null ) {
					oldEntries.clear();
					oldEntries = null;
				}
				//
				// Add ents
				for(Entry e: ents) {
					ByteBuffer buffer = null;
					try {
						buffer = ProtobufCodedOutputUtil.msgToBuffer(e);
						buffer.flip();
						//
						int dataLength = buffer.limit();
						long position = this.fileBuffer.writeByteBuffer( buffer );
						this.entries.add( new EntryLocation(e.getTerm(), e.getIndex(), position, dataLength));
					} catch (IOException e1) {
						throw new RaftException("append err:", e1);
					} finally {
						if ( buffer != null )
							NetSystem.getInstance().getBufferPool().recycle( buffer );
					}
				}
			} else if (this.entries.size() == offset) {
				//
				// offset刚好等于当前数据量，说明传入的数据刚好紧挨着当前的数据，所以直接添加进来就好了
				//
				// Add all entries
				for(Entry e: ents) {
					ByteBuffer buffer = null;
					try {
						buffer = ProtobufCodedOutputUtil.msgToBuffer(e);
						buffer.flip();
						//
						int dataLength = buffer.limit();
						long position = this.fileBuffer.writeByteBuffer( buffer );
						this.entries.add( new EntryLocation(e.getTerm(), e.getIndex(), position, dataLength));
						//
					} catch (IOException e1) {
						throw new RaftException("append err:", e1);
					} finally {
						if ( buffer != null )
							NetSystem.getInstance().getBufferPool().recycle( buffer );
					}
				}
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
			// 小于当前索引，说明已经被压缩过了
			long offset = this.entries.get(0).getIndex();
			if (compactIndex <= offset)
				throw new Errors.RaftException(Errors.ErrCompacted);
			//
			// 大于当前最大索引，panic
			long lastIndex = lastIndex();
			if (compactIndex > lastIndex)
				throw new Errors.RaftException("compact " + compactIndex + " is out of bound lastindex(" + lastIndex + ")");
			//
			int i = (int) (compactIndex - offset);
			//
			// @see https://github.com/lichuang/etcd-3.1.10-codedump/blob/master/raft/storage.go
			//
			List<EntryLocation> oldEntries = this.entries;
			EntryLocation oldLocation = oldEntries.get(i);
			long oldPosition = oldLocation.getPosition();
			long oldTerm = oldLocation.getTerm();
			long oldIndex = oldLocation.getIndex();
			
			///
			// 先写一个空数据 &  再把老的 location append进来
			List<EntryLocation> newEntries = new ArrayList<>();
			newEntries.add( new EntryLocation(oldTerm, oldIndex, 0, 0) ); // Null
			for(int idx = i+1; idx < oldEntries.size(); idx++) 
				newEntries.add( oldEntries.get( idx ) );		
			//
			this.entries = newEntries;
			//
			// clear to let GC do its work
			if ( oldEntries != null ) {
				oldEntries.clear();
				oldEntries = null;
			}
			//
			this.fileBuffer.compact( oldPosition );
			
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
			if( appliedIndex < snapMetadata.getIndex() )
				throw new Errors.RaftException(Errors.ErrSnapOutOfDate);
	
			// 传入的索引不能超过最大索引
			if( appliedIndex > lastIndex() ) 
				throw new Errors.RaftException("snapshot " + appliedIndex + " is out of bound lastindex(" + lastIndex() + ")");
			//
			long index = this.entries.get(0).getIndex();
			long term = this.entries.get( (int) (appliedIndex - index) ).getTerm();
			//
			//  Snapshot is built here for it.
			// -----------------------------------------------------------
			//
			// Metadata
			SnapshotMetadata.Builder metadataBuilder = SnapshotMetadata.newBuilder();
			metadataBuilder.setIndex(appliedIndex);
			metadataBuilder.setTerm(term);
			if( cs != null ) 
				metadataBuilder.setConfState(cs);
			SnapshotMetadata metadata = metadataBuilder.build();
			//
			// Chunk
			SnapshotChunk.Builder chunkBuilder = SnapshotChunk.newBuilder();
			chunkBuilder.setSeqNo( seqNo );
			chunkBuilder.setLast( last );
			if( data != null)
				chunkBuilder.setData( ZeroByteStringHelper.wrap(data) );
			SnapshotChunk chunk = chunkBuilder.build();
			//
			Snapshot snapshot = Snapshot.newBuilder()	//
					.setMetadata( metadata )	//
					.setChunk( chunk )	//
					.build();
			
			// 更新 Snapshot 元数据
			this.snapMetadata = metadata;
			//
			return  snapshot;
		} finally {
			readLock.unlock();
		}
	}
	
	@Override
	public void close() {
		writeLock.lock();
		try {
			//
			if (fileBuffer != null)
				fileBuffer.close();
		} finally {
			writeLock.unlock();
		}
	}
}