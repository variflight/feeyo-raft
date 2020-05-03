package com.feeyo.raft;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Util;

// @see https://github.com/pingcap/raft-rs/blob/master/src/log_unstable.rs

//
// unstable用来保存还未持久化的数据
//	其中又包括两部分， 前半部分是快照数据，而后半部分是日志条目组成的数组entries， 另外 offset 保存的是 entries 数组中的第一条数据在raft日志中的索引，
//  即第 i 条 entries 数组数据在 raft 日志中的索引为 i + offset
//
// ----------------------------------------------------------------
// |							offset							  |
// ----------------------------------------------------------------
// |	   snapshot				  |			  entries  			  |
// ----------------------------------------------------------------
//
// 注：offset可能小于持久化存储的最大索引偏移量，这意味着持久化存储中在offset的数据可能被截断
//
// 对Leader节点来说，它维护了客户端的更新请求对应的日志项
// 对Follower节点，它维护的是Leader节点复制的日志项
//
public class Unstable {
	
	private static Logger LOGGER = LoggerFactory.getLogger( Unstable.class );
	
	// 还没有持久化的快照，指向元数据
	private volatile SnapshotMetadata snapshotMeta;

	// 还未持久化的数据
	private List<Entry> entries;

	// offset用于保存entries数组中的数据的起始index
	private long offset;

	public Unstable(long offset) {
		this.offset = offset;
		//
		this.snapshotMeta = null;
		this.entries = new ArrayList<Entry>();
    }
	
	// 返回第一条数据索引, 只有当快照数据存在的时候才能拿到第一条数据索引
	public long maybeFirstIndex() {
		return this.snapshotMeta == null ? Const.ZERO_IDX : this.snapshotMeta.getIndex() + 1;
	}

	// 返回最后一条数据的索引，因为entries数据在后，而快照数据在前，
	// 所以取最后一条数据索引是从entries开始查，查不到的情况下才查快照数据
	public long maybeLastIndex() {
		//
		if ( Util.isNotEmpty( this.entries ) ) 
			return this.offset + this.entries.size() - 1;

		if ( this.snapshotMeta != null ) 
			return this.snapshotMeta.getIndex();

		return  Const.ZERO_IDX;
	}
	
	// 根据日志数据索引，得到这个日志对应的任期号
	public long maybeTerm(long index) {

		if (index < this.offset) {
			// 如果索引比 offset 小，那么尝试从快照中获取
			if( this.snapshotMeta != null && index == this.snapshotMeta.getIndex()) {
				// 只有在正好快照meta数据的index的情况下才查得到，在这之前的都查不到term了
				return this.snapshotMeta.getTerm();
			}
			return Const.ZERO_TERM;
		}
		
		// 到了这里就是 index >= offset 的情况了
		long last = maybeLastIndex();
		
		// 如果比lastIndex还大，那也查不到
		if (last == Const.ZERO_IDX || index > last) {
			return Const.ZERO_TERM;
		}
		
		return this.entries.get((int) (index - this.offset)).getTerm();
	}

	// 传入索引 index 和 term，表示目前这块数据应用层面已经持久化了，
	// unstable 在满足任期号相同以及 index 大于等于offset的情况下，可以将entries中的数据进行缩容，将index之前的数据删除
	public void stableTo(long index, long term) {

		long t = this.maybeTerm(index);
		if (t == Const.ZERO_TERM)
			return;
		
		// 只有在term相同，同时索引大于等于当前offset的情况下
		if (t == term && index >= this.offset) {
			// 因为前面的数据被持久化了，所以将entries缩容，从index开始
			int start = (int) (index + 1 - this.offset);
			List<Entry> list = Util.slice(this.entries, start, this.entries.size());
			if (list == null) {
				this.entries.clear();
			} else {
				this.entries = list;
			}
			// offset也要从index开始
			this.offset = index + 1;
		}
	}
	
	// 传入索引index，表示索引 index 对应的快照数据已经被应用层持久化了，如果 index 等于快照的索引，那么快照就可以被置空了
	public void stableSnapTo(long index) {
		//
		if (this.snapshotMeta != null && index == this.snapshotMeta.getIndex())  {
			this.snapshotMeta = null;
		}
	}

	// 从快照中恢复
	public void restore(SnapshotMetadata meta) {
		// 偏移量从快照索引之后开始，entries置空
		this.offset = meta.getIndex() + 1;
		// 清空entries数组
		this.entries.clear();
		
		// 保存到snapshot中，注意这里保存的是指针，因为快照数据可能很大，如果值拷贝可能会涉及很多的数据
		this.snapshotMeta = meta;
	}
	
	// 传入的数据跟现有的entries数据可能有重合的部分, 需要根据 offset与传入数据的索引大小关系进行处理，有些数据可能会被截断
	public void truncateAndAppend(List<Entry> ents) throws RaftException {
		//
		if ( Util.isEmpty(ents) )
			return;
		
		// 先拿到这些数据的第一个索引
		long after = ents.get(0).getIndex();
		if (after == this.offset + this.entries.size()) {
			// 如果正好是紧接着当前数据的，就直接append
			this.entries.addAll(ents);
			
		} else if (after <= this.offset) {
			LOGGER.info("replace the unstable entries from index {}", after);
			// 如果比当前偏移量小，那用新的数据替换当前数据，需要同时更改offset和entries
			this.offset = after;
			this.entries.clear();
			this.entries.addAll(ents);
			
		} else {
			// truncate to after and copy to self.entries then append
			LOGGER.info("truncate the unstable entries before index {}", after);
			
			// 到了这里，说明 u.offset < after < u.offset+uint64(len(u.entries))
			// 那么新的entries需要拼接而成
			List<Entry> list = this.slice(offset, after);
			if (list == null) {
				this.entries.clear();
			} else {
				this.entries = list;
			}
			this.entries.addAll(ents);
		}
	}

	// 返回索引范围在[ low-offset : high-offset ]之间的数据
	public List<Entry> slice(long low, long high) throws RaftException {
		this.mustCheckOutOfBounds(low, high);
		//
		return Util.slice(this.entries, (int) (low - offset), (int) (high - offset));
	}

	// 检查传入的索引范围是否合法
	private void mustCheckOutOfBounds(long low, long high) throws RaftException {
		if (low > high) {
			throw new Errors.RaftException("invalid unstable.slice " + low + " > " + high);
		}
		//
		long upper = this.offset + this.entries.size();
		if (low < offset || high > upper) {
			throw new Errors.RaftException("unstable.slice[" + low + "," + high + ") out of bound [" + offset + "," + upper + "]");
		}
	}

	public SnapshotMetadata getSnapshotMetadata() {
		return this.snapshotMeta;
	}

	public List<Entry> getEntries() {
		return this.entries;
	}

	public long getOffset() {
		return this.offset;
	}
}
