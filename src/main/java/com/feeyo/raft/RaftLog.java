package com.feeyo.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.storage.Storage;
import com.feeyo.raft.util.Util;

/**
 * Raft log implementation
 * 
 * @see https://github.com/etcd-io/etcd/blob/master/raft/log.go
 * @see https://github.com/pingcap/raft-rs/blob/master/src/raft_log.rs
 * @see https://github.com/coreos/etcd/blob/master/raft/log.go
 * 
 *  RaftLog中，几个部分的数据排列
 *	------------------------------------------------------------------------------------------------
 *	|							 firstIndex						    lastIndex				       |
 *	------------------------------------------------------------------------------------------------
 *	|	   Storage.snapshot	 		|      Storage.entries      	   |     Unstable.entries      |
 * 	|	   						持久化数据区域      				   	   |     未持久化数据区域         |
 *	------------------------------------------------------------------------------------------------
 *	|	    				  RaftLog.committed				     Unstable.offset                   |     
 *  |	    				  RaftLog.applied				                                       |
 *  ------------------------------------------------------------------------------------------------
 *  
 *  TODO：
 *  只考虑log entries的话，unstable是未落盘的，WAL是已落盘entries，storage是访问已落盘数据的interface，具体实现上，一般是WAL加某种cache的实现
 *  etcd自带的memoryStorage实现这个storage接口，但比较简单，是没有被compact掉的已落盘entries在内存的一份拷贝，和传统意义cache不同，因为它有已落盘未compact掉的所有数据
 *  unstable不是复制数据的来源，在有follower落后、刚重启、新join的情况下，给这类follower的数据多数来自已落盘部分
 *
 *  CockroachDB使用一个基于llrb的LRU cache来替代 memoryStorage这个东西，WAL部分是 rocksdb。
 * 
 * @author zhuam
 *
 */
public class RaftLog {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftLog.class );
	//
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	protected final Lock writeLock = readWriteLock.writeLock();
	protected final Lock readLock = readWriteLock.readLock();
	//
	private Storage storage;			// 用于保存自从最后一次snapshot之后提交的数据
	private Unstable unstable;  		// 用于保存还没有持久化的数据和快照，这些数据最终都会保存到storage中
	//
	private volatile long committed; 	// committed 是写入持久化存储中的最高index
	private volatile long applied;		// applied 是应用到状态机中的最高index (TODO:一条日志首先要提交成功（即committed），才能被applied到状态机中， 因此不等式一直成立：applied <= committed)
	private long maxNextEntsSize;  		// maxNextEntsSize 是消息的最大聚合字节大小数
	//
	private RaftNodeListener raftNodeListener;

	public RaftLog(RaftNodeListener listener, Storage storage, long maxNextEntsSize) throws RaftException {
		if (storage == null)
			throw new Errors.RaftException("storage must not be nil");
		//
		this.maxNextEntsSize = maxNextEntsSize;
		this.storage = storage;
		long firstIndex = this.storage.firstIndex();
		long lastIndex = this.storage.lastIndex();
		this.unstable = new Unstable(lastIndex + 1); // TODO：offset从持久化之后的最后一个index的下一个开始
		//
		// Initialize our committed and applied pointers to the time of the last compaction.
		this.committed = firstIndex - 1;
		this.applied = firstIndex - 1;
		this.raftNodeListener = listener;
		LOGGER.info("init raftLog, firstIndex={}, lastIndex={}", firstIndex, lastIndex);
	}

	/**
	 * 返回 Storage
	 * @return
	 */
	public Storage getStorage() {
		return this.storage;
	}

	/**
	 * 返回最后一个索引的term
	 * @return
	 * @throws RaftException
	 */
	public long lastTerm() throws RaftException {
		long lastIndex = this.lastIndex();
		return this.term(lastIndex);
	}

	/**
	 * 返回index对应的term
	 * @param index
	 * @return 
	 * @throws RaftException
	 */
	public long term(long index) throws RaftException {
		readLock.lock();
		try {
			// the valid term range is [index of dummy entry, last index]
			long dummyIndex = this.firstIndex() - 1;
			if (index < dummyIndex || index > this.lastIndex()) {  // 先判断范围是否正确
				return Const.ZERO_TERM;	// TODO: return an error instead?
			}
			long term = this.unstable.maybeTerm(index);	// 先尝试从unstable中查找term
			if (term == Const.ZERO_TERM)
				term = this.storage.getTerm(index); // 从storage中查找term
			return term;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * Returns the first index in the store that is available via entries
	 * @return
	 */
	public long firstIndex() {
		readLock.lock();
		try {
			// 首先尝试在未持久化数据中看有没有快照数据, 否则返回持久化数据的 firstIndex
			long index = this.unstable.maybeFirstIndex();
			if (index == Const.ZERO_IDX) 
				index = this.storage.firstIndex();
			return index;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * Returns the last index in the store that is available via entries
	 * @return
	 */
	public long lastIndex() {
		readLock.lock();
		try {
			long index = this.unstable.maybeLastIndex();
			if (index == Const.ZERO_IDX) 
				index = this.storage.lastIndex();
			return index;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 查找冲突的索引 （ 一个日志条目在其索引值对应的term与当前相同索引的term不相同时认为是有冲突的数据 ） 
	 *  1、如果有冲突， 它返回现有Entry 与给定 Entry 之间存在冲突 Entry 的第一个索引
	 *  2、如果没有冲突，当前存在的日志条目包含所有传入的日志条目，返回0
	 *  3、如果没有冲突，但传入的日志条目有新的数据，则返回新日志条目的第一条索引
	 * @param ents
	 * @return
	 * @throws RaftException
	 */
	private long findConflict(List<Entry> ents) throws RaftException {
		// 遍历传入的ents数组
		for (Entry ent: ents) {
			// 找到第一个任期号不匹配的，即当前在raftLog存储的该索引数据的任期号，不是ent数据的任期号
			if (!this.matchTerm(ent.getIndex(), ent.getTerm())) {
				if (ent.getIndex() <= lastIndex()) {
					// 如果不匹配任期号的索引数据，小于当前最后一条日志索引，就打印错误日志
					LOGGER.info("found conflict at index {}, [existing term:{}, conflicting term:{}]", 
							ent.getIndex(),
							zeroTermOnErrCompacted(ent.getIndex()), ent.getTerm());
				}
				// 返回第一条任期号与索引号不匹配的数据索引
				return ent.getIndex();
			}
		}
		return 0;
	}

	/**
	 * 判断索引 index 的 term 是否和 term 一致
	 * @param index
	 * @param term
	 * @return
	 */
	public boolean matchTerm(long index, long term) {
		try {
			long t = this.term(index);
			return t == term;
		} catch (Errors.RaftException e) {
			return false;
		}
	}

	public boolean maybeCommit(long maxIndex, long term) throws RaftException {
		// 只有在传入的 index 大于当前 commit 索引，以及 maxIndex 对应的 term 与传入的 term 匹配时，才使用这些数据进行commit
		if (maxIndex > this.committed && zeroTermOnErrCompacted( maxIndex ) == term) {
			this.commitTo( maxIndex );
			return true;
		}
		return false;
	}

	/**
	 * 尝试添加一组日志，如果不能添加则返回 (0或异常)，否则返回 (last index of new entries)
	 * @param index 从哪里开始的日志条目
	 * @param term 这一组日志对应的term
	 * @param committed leader上的committed索引
	 * @param ents  需要提交的一组日志，因此这组数据的最大索引为 index+len(ents)
	 * @return last index of new entries
	 * @throws RaftException
	 */
	public long maybeAppend(long index, long term, long committed, List<Entry> ents) throws RaftException {
		writeLock.lock();
		try {
			// 首先需要保证传入的 index 和 term 能匹配，不匹配直接返回 0
			if ( !this.matchTerm(index, term) ) 
				return Const.ZERO_IDX;
			//
			long lastNewIndex = index + ents.size();  // 首先得到传入数据的最后一条索引
			long ci = this.findConflict(ents);		// 查找传入的数据从哪里开始找不到对应的Term了
			if ( ci == 0 ) {
				// 没有冲突，忽略
			} else if(ci <= this.committed)  {
				// 找到的数据索引小于committed，说明传入的数据是错误的
				throw new Errors.RaftException(String.format("entry %s conflict with committed entry [committed(%s)]", ci, this.committed));
				
			} else {
				// ci > 0的情况下来到这里
				long offset = index + 1;
				
				// 从查找到的数据索引开始，将这之后的数据放入到 unstable 存储中
				List<Entry> ents2 = Util.slice(ents, (int) (ci - offset), ents.size());
				if ( Util.isNotEmpty(ents2) ) {
					// 如果索引小于committed，则说明该数据是非法的
					long after = ents2.get(0).getIndex() - 1;
					if (after < this.committed) 
						 throw new Errors.RaftException(String.format("after(%s) is out of range [committed(%s)]", after, this.committed));
					// 放入unstable存储中
					this.unstable.truncateAndAppend(ents2);
				}
			}
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("commitTo, ci={}, committed={}, lastNewIndex={}", ci, committed, lastNewIndex);
			}
			
			// 选择 committed 和 lastNewIndex 中的最小者进行 commit
			this.commitTo ( Math.min(committed, lastNewIndex) );
			return lastNewIndex;
			
		} finally {
			writeLock.unlock();
		}
	}
	
	/**
	 * 添加 entries 到 unstable，返回最后一条日志的索引
	 * @param term
	 * @param es
	 * @return
	 * @throws RaftException
	 */
	public long append(long term, List<Entry> es) throws RaftException {
		if (Util.isEmpty(es))
			return lastIndex(); // 没有数据，直接返回最后一条日志索引
		//
		writeLock.lock();
		try {
			List<Entry> entries = new ArrayList<Entry>();
			long lastIndex = lastIndex();
			for(int i = 0; i< es.size(); i++) {
				// 设置这些entries的Term以及index
				Entry e = es.get(i).toBuilder() //
							.setTerm(term) //
							.setIndex(lastIndex + 1 + i) //
							.build(); //
				entries.add(e);
			}
			//
			// 如果索引小于committed，则说明该数据是非法的
			long after = entries.get(0).getIndex() - 1;
			if (after < this.committed) 
				 throw new Errors.RaftException(String.format("after(%s) is out of range [committed(%s)]", after, this.committed));
			//
			this.unstable.truncateAndAppend(entries); // 放入unstable存储中
			return lastIndex();
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 将 RaftLog 的commit索引，修改为tocommit
	 * @param tocommit
	 * @throws RaftException
	 */
	public void commitTo(long tocommit) throws RaftException {
		if (this.committed >= tocommit) // 首先判断，commit不能变小
			return;
		//
		if (lastIndex() < tocommit)  // commit如果比lastIndex大则是非法的
			throw new Errors.RaftException(String.format("tocommit(%s) is out of range [lastIndex(%s)]. Was the raft log corrupted, truncated, or lost?", tocommit, lastIndex()));
		this.committed = tocommit;
	}

	/**
	 * 修改 applied 索引
	 * @param index
	 * @throws RaftException
	 */
	public void appliedTo(long index) throws RaftException {
		if (index == Const.ZERO_IDX)
			return;
		//
		// 判断合法性
		// 新的 applied index 既不能比 committed 大，也不能比当前的applied索引小
		if (this.committed < index || index < applied) {	
			throw new Errors.RaftException(String.format("applied(%s) is out of range [prevApplied(%s), committed(%s)]", index, applied, this.committed));
		} else {
			this.applied = index;
			this.raftNodeListener.onAppliedIndex( index );
		}
	}
	
	/**
	 * TODO: 该方法仅限初始化设置使用，后续的 committed 变更需要走 commitTo()
	 * @param committed
	 */
	public void setCommitted(long committed) {
		this.committed = committed;
	}
	
	public void setApplied(long index)  {
		if (index == Const.ZERO_IDX)
			return;
		this.applied = index;
		this.raftNodeListener.onAppliedIndex(index);
	}

	/**
	 * 传入数据索引
	 * 该索引表示在这个索引之前的数据应用层都进行了持久化，修改unstable的数据
	 * @param index
	 * @param term
	 */
 	public void stableTo(long index, long term) {
 		writeLock.lock();
 		try {
 			this.unstable.stableTo(index, term);
 		} finally {
 			writeLock.unlock();
 		}
	}

 	/**
 	 * 传入数据索引
 	 * 该索引表示在这个索引之前的数据应用层都进行了持久化，修改unstable的快照数据
 	 * @param index
 	 */
	public void stableSnapTo(long index) {
		writeLock.lock();
		try {
           this.unstable.stableSnapTo(index);
		} finally {
			writeLock.unlock();
		}
	}
	
	/**
	 * 返回 unstable 是否有存储的数据
	 * @return
	 */
	public boolean hasUnstableEntries() {
		readLock.lock();
		try {
			if (unstable != null && Util.isNotEmpty(unstable.getEntries()))
				return true;
			return false;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 返回 unstable 存储的数据
	 * @return
	 */
	public List<Entry> unstableEntries() {
		readLock.lock();
		try {
			if (unstable != null && Util.isNotEmpty(unstable.getEntries())) {
				List<Entry> unstableEntries = this.unstable.getEntries(); // clone
				return Util.slice(unstableEntries, 0, unstableEntries.size());
			}
			return null;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 获取从index 开始的 entries，大小不超过 maxSize
	 * @param index
	 * @param maxSize
	 * @return
	 * @throws RaftException
	 */
	public List<Entry> entries(long index, long maxSize) throws RaftException {
		long lastIndex = this.lastIndex();
		if (index > lastIndex) 
			return Collections.emptyList();
		//
		return this.slice(index, lastIndex + 1, maxSize);
	}

	/**
	 * 返回所有 entries
	 * @return
	 * @throws RaftException
	 */
	public List<Entry> allEntries() throws RaftException {
		long firstIndex = this.firstIndex();
		return this.entries(firstIndex, Long.MAX_VALUE);
	}

	/**
	 * 判断是否比当前节点的日志更新
	 * 	1）term是否更大 
	 * 	2）term相同的情况下，索引是否更大
	 * @param lastIndex
	 * @param term
	 * @return
	 * @throws RaftException
	 */
	public boolean isUpToDate(long lastIndex, long term) throws RaftException {
		// TODO: In some situation, when preVote is disabled,
		//  an outdated candidate can have a pretty big term
		return term > this.lastTerm() 
				|| (term == this.lastTerm() && lastIndex >= this.lastIndex());
	}

	
	// 返回commit但是还没有apply的所有数据
	
	/// Returns all the available entries for execution.
    /// If applied is smaller than the index of snapshot, it returns all committed
    /// entries after the index of snapshot.
	public List<Entry> nextEntries() throws RaftException {
		return this.nextEntriesSince( applied );
	}
	//
	// Returns any entries since the a particular index.
	public List<Entry> nextEntriesSince(long sinceIndex) throws RaftException {
		long offset = Math.max(sinceIndex + 1, this.firstIndex()); // 首先得到 applied和 firstIndex的最大值
		if (this.committed + 1 > offset) {
			return this.slice(offset, this.committed + 1, maxNextEntsSize); 
		}
		return null;
	}
	
	/**
	 * 这个函数的功能跟前面 nextEntries 类似，只不过这个函数做判断而不返回实际数据
	 * @return
	 */
	public boolean hasNextEntries() {
		return this.hasNextEntriesSince( applied );
	}

	/**
	 * 返回 sinceIndex 索引以来是否存在条目
	 * @param sinceIndex
	 * @return
	 */
	public boolean hasNextEntriesSince(long sinceIndex) {
		long offset = Math.max(sinceIndex + 1, this.firstIndex());
		return this.committed + 1 > offset;
	}

	/**
	 * 返回快照的元数据
	 * @return
	 */
	public SnapshotMetadata getSnapshotMetadata() {
		readLock.lock();
		try {
			// 如果没有保存的数据有快照，就返回
//			SnapshotMetadata meta = this.unstable.getSnapshotMetadata();
//			if ( meta != null ) 
//				return meta;
			
			// 否则返回持久化存储的快照数据
			return this.storage.getSnapshotMetadata();
		} finally {
			readLock.unlock();
		}
	}
	
	/**
	 * TODO: 此处，后续需要考虑 ready 提取的 readonly
	 * @return
	 */
	public SnapshotMetadata getUnstableSnapshotMetadata() {
		return this.unstable.getSnapshotMetadata();
	}
	
	/**
	 * 是否存在 snapshot
	 * @return
	 */
	public boolean hasUnstableSnapshotMetadata() {
		SnapshotMetadata meta = this.unstable.getSnapshotMetadata();
		return meta != null && meta.getIndex() != 0;
	}
	
	/**
	 * 检查leader 是否在当前term有过commit entry
	 * @param logIndex
	 * @return
	 * @throws RaftException
	 */
	public long zeroTermOnErrCompacted(long logIndex) throws RaftException {
		try {
			return term(logIndex);
		} catch (Errors.RaftException e) {
			if (Errors.ErrCompacted.equalsIgnoreCase(e.getMessage()))
				return 0L;
			throw new Errors.RaftException("unexpected error", e);
		}
    }
	
	
	/**
	 * 判断传入的 low，high 是否超过范围了
	 * @param low
	 * @param high
	 * @throws RaftException
	 */
	private void mustCheckOutOfBounds(long low, long high) throws RaftException {
		if (low > high) {
			throw new Errors.RaftException(String.format("invalid slice %s > %s", low, high));
		}
		//
		long firstIndex = this.firstIndex();
		if (low < firstIndex) {
			throw new Errors.RaftException(String.format("%s, slice[%s,%s], firstIndex=%s, unstable[%s,%s], store[%s,%s]",
					Errors.ErrCompacted, low, high, firstIndex,
					this.unstable.maybeFirstIndex(), this.unstable.maybeLastIndex(), 
					this.storage.firstIndex(), this.storage.lastIndex()));
		}
		//
		long lastIndex = this.lastIndex();
		long length = lastIndex + 1 - firstIndex;
		if (low < firstIndex || high > firstIndex + length) {
			throw new Errors.RaftException(String.format("slice[%s,%s) out of bound [%s,%s], unstable[%s,%s], store[%s,%s]", 
					low, high, firstIndex, lastIndex, 
					this.unstable.maybeFirstIndex(), this.unstable.maybeLastIndex(),
					this.storage.firstIndex(), this.storage.lastIndex()));
		}
	}


	/**
	 * 返回 [low,high-1] 之间的数据，这些数据的大小总和不超过 maxSize
	 * @param low
	 * @param high
	 * @param maxSize
	 * @return
	 * @throws RaftException
	 */
	public List<Entry> slice(long low, long high, long maxSize) throws RaftException {
		readLock.lock();
		try {
			mustCheckOutOfBounds(low, high);
			//
			if (low == high)
				return Collections.emptyList();
			//
			List<Entry> entries = null;
			long offset = this.unstable.getOffset();
			if (low < offset ) {
				try {
					// low 小于 unstable 的 offset，说明前半部分在持久化的storage中
					// (TODO: 注意传入storage.getEntries 的 high 参数取 high 和 unstable offset的较小值)
					entries = this.storage.getEntries(low, Math.min(high, offset), maxSize);
					if (entries == null) 
						return Collections.emptyList();
					//
					// check if ents has reached the size limitation
					if (entries.size() < Math.min(high, offset) - low) 
						return entries;
					//
				} catch (Errors.RaftException e) {
					if (Errors.ErrUnavailable.equals(e.getMessage()))
						LOGGER.warn("entries[{}:{}] is unavailable from storage", low, Math.min(high, offset));
					throw e;
				}
			}
	
			//
			if (high > offset) {
				// high 大于 unstable offset，说明后半部分在unstable中取得
				List<Entry> unstableEnts = this.unstable.slice(Math.max(low, offset), high);
				if ( entries != null && Util.len(entries) > 0 ) {
					entries.addAll(unstableEnts);
				} else {
					entries = unstableEnts;
				}
			}
			//
			if (entries == null)
				return Collections.emptyList();
			return Util.limitSize(entries, maxSize);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 使用快照数据进行恢复
	 * @param meta
	 */
	public void restore(SnapshotMetadata meta) {
		writeLock.lock();
		try {
			LOGGER.info("log [{}] starts to restore snapshot [index:{}, term:{}]", toString(), meta.getIndex(), meta.getTerm());
			this.committed = meta.getIndex();
			this.unstable.restore( meta );
		} finally {
			writeLock.unlock();
		}
	}

	public long getCommitted() {
		return this.committed;
	}

	public long getApplied() {
		return this.applied;
	}

	@Override
	public String toString() {
		StringBuffer strSb = new StringBuffer();
		strSb.append("[");
		strSb.append("committed:").append(committed);
		strSb.append(",applied:").append(applied);
		if (unstable != null) {
			strSb.append(", unstable(offset:").append(unstable.getOffset());
			strSb.append(",size:").append(unstable.getEntries().size());
			strSb.append(",first:").append(unstable.maybeFirstIndex());
			strSb.append(",last:").append(unstable.maybeLastIndex());
			strSb.append(")");
		}
		if (storage != null) {
			strSb.append(", storage(first:").append(storage.firstIndex());
			strSb.append(",last:").append(storage.lastIndex());
			strSb.append(")");
		}
		strSb.append("]");
		return strSb.toString();
	}
}