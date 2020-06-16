package com.feeyo.raft;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Errors.RaftException;

// @see https://github.com/pingcap/raft-rs/blob/master/src/progress.rs
// @see https://github.com/coreos/etcd/blob/master/raft/design.md
//
// 该数据结构用于在leader中保存每个follower的状态信息，leader将根据这些信息决定发送给节点的日志
//
public class Progress {
	
	// Next保存的是下一次leader发送append消息时传送过来的日志索引
	// 当选举出新的leader时，首先初始化Next为该leader最后一条日志+1
	// 如果向该节点append日志失败，则递减Next回退日志，一直回退到索引匹配为止

	// Match保存在该节点上保存的日志的最大索引，初始化为0
	// 正常情况下，Next = Match + 1
	// 以下情况下不是上面这种情况：
	// 1. 切换到Probe状态时，如果上一个状态是Snapshot状态，即正在接收快照，那么Next = max(pr.Match+1, pendingSnapshot+1)
	// 2. 当该follower不在Replicate状态时，说明不是正常的接收副本状态。
	//    此时当leader与follower同步leader上的日志时，可能出现覆盖的情况，即此时follower上面假设Match为3，但是索引为3的数据会被
	//    leader覆盖，此时Next指针可能会一直回溯到与leader上日志匹配的位置，再开始正常同步日志，此时也会出现Next != Match + 1的情况出现
	//
	private volatile long matched;		
	private volatile long nextIdx;		
	
	// ProgressState.Probe： leader每个心跳间隔最多发送一条复制消息该节点。 它还探讨了 follower 的实际进展
	// ProgressState.Replicate：正常的接受副本数据状态，leader在发送复制消息后，修改该节点的next索引为发送消息的最大索引+1，
	// 注：这是一种优化状态，用于快速将日志条目复制到关注者
	// ProgressState.Snapshot：接收副本状态, leader应该先发送快照并停止发送任何复制消息
	//
	private volatile ProgressState state;
	
	// 是否暂停给这个副本发送 MsgAppend 
	//
	// 在状态切换到 ProgressState.Probe 状态以后，该follower就标记为Paused，此时将暂停同步日志到该节点
	private volatile boolean paused;
	
	// 如果向该节点发送快照消息，pendingSnapshot用于保存快照消息的索引
	// 当 pendingSnapshot 不为0时，该节点也被标记为暂停状态。
	// raft只有在这个正在进行中的快照同步失败以后，才会重传快照消息
	private long pendingSnapshot;
	
	
	// recentActive 标志位是leader节点在收到 follower的消息响应后被设置为true的，
	//  这些响应包括心跳响应（MsgHeartbeatResponse）与正常的数据同步响应（ MsgAppendResponse ）
	//  这些响应保证了 follower 仍然认可其Leader的身份
	private volatile boolean recentActive;
	private volatile long recentActiveTime;
	
	// 用于实现滑动窗口，用来做流量控制
	private Inflights ins;

	// 是否为 learner
	private boolean isLearner;
	
	
	// ProgressState.Replicate 状态是最容易理解的，Leader 可以给对应的副本发送多个 MsgAppend 消息（不超过滑动窗口的限制），并适时地将窗口向前滑动。
	// 然而，我们注意到，在 Leader 刚选举出来时，Leader 上面的所有其他副本的状态却被设置成了 Probe。这是为什么呢？
	//
	// 当某个副本处于 Probe 状态时，Leader 只能给它发送 1 条 MsgAppend 消息。这是因为，
	// 在这个状态下的 Progress 的 next_idx 是 Leader 猜出来的，而不是由这个副本明确的上报信息推算出来的。它有很大的概率是错误的，亦即 Leader 很可能会回退到某个地方重新发送；
	// 甚至有可能这个副本是不活跃的，那么 Leader 发送的整个滑动窗口的消息都可能浪费掉。
	//
	// 因此，我们引入 Probe 状态，当 Leader 给处于这一状态的副本发送了 MsgAppend 时，这个 Progress 会被暂停掉（源码片段见上一节），
	// 这样在下一次尝试给这个副本发送 MsgAppend 时，会在 Raft.sendAppend 中跳过。
	// 而当 Leader 收到了这个副本上报的正确的 last index 之后，Leader 便知道下一次应该从什么位置给这个副本发送日志了，这一过程在 Progress.maybeUpdate 方法中：
	//
	
	// ProgressState.Snapshot 状态与 Progress 中的 pause 标志十分相似，一个副本对应的 Progress 一旦处于这个状态，Leader 便不会再给这个副本发送任何 MsgAppend 了。
	// 但是仍有细微的差别：事实上在 Leader 收到 MsgHeartbeatResponse 时，也会调用 Progress.resume 来将取消对该副本的暂停，然而对于 ProgressState.Snapshot 状态的 Progress 则没有这个逻辑。
	// 这个状态会在 Leader 成功发送完成 Snapshot，或者收到了对应的副本的最新的 MsgAppendResponse 之后被改变，详细的逻辑请参考源代码
	//
	//
	// 
	//
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
	
	public Progress(long nextIdx, Inflights ins) {
		this(nextIdx, ins, false);
	}
	
	public Progress(long nextIdx, Inflights ins, boolean isLearner) {
		this.nextIdx = nextIdx;
		this.ins = ins;
		this.isLearner = isLearner;
		this.state = ProgressState.Probe;
	}
	
	// 重置状态
	private void resetState(ProgressState state) {
		this.paused = false;
		this.pendingSnapshot = 0;
		this.state = state;
		this.ins.reset();
	}
	
	// 修改为 Probe 状态
	public synchronized void becomeProbe() {
		// If the original state is ProgressState.Snapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1
		//
		if ( this.state == ProgressState.Snapshot ) {
			 // 如果当前状态是接受快照状态，那么可以知道该节点已经成功接受处理了该快照，此时修改next索引需要根据max和快照索引来判断
			 long pending_snapshot = this.pendingSnapshot;
			 this.resetState(ProgressState.Probe);
			 // 取两者的最大值+1
	         this.nextIdx = Math.max(this.matched + 1, pending_snapshot + 1);
		} else {
            this.resetState(ProgressState.Probe);
            this.nextIdx = this.matched + 1;
        }
	}
	
	public synchronized void becomeReplicate() {
		this.resetState( ProgressState.Replicate );
		this.nextIdx = this.matched + 1;
	}
	
	public synchronized void becomeSnapshot(long snapshotIndex) {
		this.resetState(ProgressState.Snapshot);
		this.pendingSnapshot = snapshotIndex;
	}
	
	public void snapshotFailure() {
		this.pendingSnapshot = 0;
	}
	
	// maybe_snapshot_abort unsets pendingSnapshot if Match is equal or higher than the pendingSnapshot
	//
	// 可以中断快照的情况：
	//  当前为接收快照，同时match已经大于等于快照索引
	// 		因为match已经大于快照索引了，所以这部分快照数据可以不接收了，也就是可以被中断的快照操作
	// 		因为在节点落后leader数据很多的情况下，可能leader会多次通过snapshot同步数据给节点，
	// 		而当 pr.Match >= pr.PendingSnapshot的时候，说明通过快照来同步数据的流程完成了，这时可以进入正常的接收同步数据状态了。
	public boolean maybeSnapshotAbort() {
		boolean isAbort = (this.state == ProgressState.Snapshot) && (this.matched >= this.pendingSnapshot);
		return isAbort;
	}

	// maybe_update returns false if the given n index comes from an outdated message.
    // Otherwise it updates the progress and returns true.
	//
	
	// 收到 append response 的成功应答之后，leader更新节点的索引数据
	// 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
	//
	public synchronized boolean maybeUpdate(long n) {
		boolean updated = false;
		if (matched < n) {
			matched = n;
            updated = true;
            resume(); // 取消暂停的状态
		}
		//
		if (this.nextIdx < n + 1) {
			this.nextIdx = n + 1;
		}
		return updated;
	}
	
	public synchronized void optimisticUpdate(long n) {
		 this.nextIdx = n + 1;
	}
	
	//
	// maybe_decr_to returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
	//
	// maybeDecrTo函数在传入的索引不在范围内的情况下返回false
	// 否则将把该节点的index减少到min(rejected,last)然后返回true
	// rejected是拒绝该append消息时的索引，last是拒绝该消息的节点的最后一条日志索引
	//
	public synchronized boolean maybeDecrTo(long rejected, long last) {
		
		// 如果当前在接收副本状态
		if (this.state == ProgressState.Replicate) {
			// the rejection must be stale if the progress has matched and "rejected" is smaller than "match".
			if (rejected <= this.matched) {
				// 这种情况说明返回的情况已经过期，中间有其他添加成功的情况，导致match索引递增，
				// 此时不需要回退索引，返回false
				return false;
			}
			
			// directly decrease next to match + 1
			// 否则直接修改 next 到 match+1
			this.nextIdx = this.matched + 1;
			return true;
		}
		
		// 以下都不是接收副本状态的情况
		
		// the rejection must be stale if "rejected" does not match next - 1
		// 为什么这里不是对比Match？因为Next涉及到下一次给该Follower发送什么日志，
		// 所以这里对比和下面修改的是Next索引
		// Match只表示该节点上存放的最大日志索引，而当leader发生变化时，可能会覆盖一些日志
		//
		if (this.nextIdx == 0 || this.nextIdx - 1 != rejected) {
			// 这种情况说明返回的情况已经过期，不需要回退索引，返回false
			return false;
		}

		// 到了这里就回退Next为两者的较小值
		this.nextIdx = Math.min(rejected, last + 1);
		if (this.nextIdx < 1) {
			this.nextIdx = 1;
		}
		this.resume();
		return true;
	}
	
	// 一个节点当前处于暂停状态原因有几个：
	// 		1)拒绝了最近的append消息(ProgressState.Probe状态)
	// 		2)接受snapshot状态中(ProgressState.Snapshot状态)
	// 		3)已经达到了需要限流的程度(ProgressState.Replicate状态且滑动窗口已满)
	//
	// isPaused在以下情况中返回true：
	// 		1）等待接收快照
	// 		2) inflight数组已满，需要进行限流
	// 		3) 进入ProgressState.Probe状态，这种状态说明最近拒绝了msgapps消息
	public boolean isPaused() {
		switch(state) {
		case Probe:
			return paused;
		case Replicate:
			// 如果在replicate状态，pause与否，取决于infilght数组是否满了
			return ins.full();
		case Snapshot:
			// 处理快照时一定是paused的
			return true;
		default:
			return false;
		}
	}
	
	public void resume() {
		this.paused = false;
	}
	
	public void pause() {
		this.paused = true;
	}
	
	public void setLearner(boolean isLearner) {
		this.isLearner = isLearner;
	}

	public synchronized void setMatched(long matched) {
		this.matched = matched;
	}

	public synchronized void setNextIdx(long nextIdx) {
		this.nextIdx = nextIdx;
	}

	public synchronized void setState(ProgressState state) {
		this.state = state;
	}

	public synchronized void setPendingSnapshot(long pendingSnapshot) {
		this.pendingSnapshot = pendingSnapshot;
	}

	public synchronized void setRecentActive(boolean recentActive) {
		this.recentActive = recentActive;
		if ( recentActive )
			this.recentActiveTime = TimeUtil.currentTimeMillis();
	}

	public long getMatched() {
		return matched;
	}

	public long getNextIdx() {
		return nextIdx;
	}

	public ProgressState getState() {
		return state;
	}

	public long getPendingSnapshot() {
		return pendingSnapshot;
	}

	public boolean isRecentActive() {
		return recentActive;
	}

	public long getRecentActiveTime() {
		return recentActiveTime;
	}

	public Inflights getIns() {
		return ins;
	}

	public boolean isLearner() {
		return isLearner;
	}

	///
	// E.g: 流量控制, 类似 TCP 的滑动窗口
	//
	// leader 在给某一副本发送 MsgAppend 时，会检查其对应的滑动窗口，这个逻辑在 Raft.maybeSendAppend 方法中；
	// 在收到该副本的 MsgAppendResponse 之后，会适时调用 Inflights 的 freeTo 函数，
	// 使窗口向前滑动，这个逻辑在 StepLeader 中
	//
	public static class Inflights {
        public int start;	 // the starting index in the buffer
        public int count;	 // number of inflights in the buffer
        public int size;	 // the size of the buffer
        //
        public long[] buffer = new long[0];	// buffer contains the index of the last entry inside one message.

        public Inflights(int size) {
            this.size = size;
        }

        // add adds an inflight into inflights
        public synchronized void add(long inflight) throws RaftException {
            if (full()) 
                throw new Errors.RaftException("cannot add into a full inflights");
            //
            // 获取新增消息的下标
            int next = start + count;
            if (next >= size) { //环形队列
                next -= size;
            }
            //
            // 初始化的buffer数组较短，随着使用会不断进行扩容（2倍），其扩容的上限为size
            if (next >= buffer.length) {
                growBuf();
            }
            buffer[next] = inflight; //在next位置记录消息中最后一条Entry记录的索引值
            count++;				 //递增count字段
        }

        //
        // grow the inflight buffer by doubling up to inflights.size. We grow on demand
        // instead of preallocating to inflights.size to handle systems which have
        // thousands of Raft groups per process.
        private void growBuf() {
            int newSize = buffer.length * 2;
            if (newSize == 0) {
                newSize = 1;
            } else if (newSize > size) {
                newSize = size;
            }
            //
            long[] newBuffer = new long[newSize];
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            this.buffer = newBuffer;
        }

        // free_to frees the inflights smaller or equal to the given `to` flight.
        /// 将指定消息及其之前的消息全部清空，释放 inflights 空间，让后面的消息继续发送
        public synchronized void freeTo(long to) {
        	//
        	// out of the left side of the window
        	if (count == 0 || to < buffer[start]) 	
				return;
        	//
			int idx = start;
			int i = 0;
			for (i = 0; i < count; i++) {
				if (to < buffer[idx]) 	// found the first large inflight
					break;
				//
				// increase index and maybe rotate
				idx++;
				if (idx >= size) {
					idx -= size;
				}
			}
			//
			// free i inflights and set new start index
			count -= i;
			start = idx;
			if (count == 0) {
				// inflights is empty, reset the start index so that we don't grow the buffer unnecessarily.
				start = 0;
			}
        }

        public synchronized void freeFirstOne() {
            freeTo( buffer[start] );
        }

        // full returns true if the inflights is full.
        public boolean full() {
            return count == size;
        }

        //  resets frees all inflights.
        public void reset() {
            count = 0;
            start = 0;
        }
    }

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Progress [matched=").append(matched);
		sb.append(", nextIdx=").append(nextIdx);
		sb.append(", state=").append(state);
		sb.append(", paused=").append(paused);
		sb.append(", pendingSnapshot=").append(pendingSnapshot);
		sb.append(", recentActive=").append(recentActive);
		sb.append(", isLearner=").append(isLearner);
		sb.append("]");
		return sb.toString();
	}
}