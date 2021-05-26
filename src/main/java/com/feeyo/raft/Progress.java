package com.feeyo.raft;

import com.feeyo.net.nio.util.TimeUtil;

/**
 * @see https://github.com/tikv/raft-rs/blob/master/src/tracker/progress.rs
 * @see https://github.com/etcd-io/etcd/blob/main/raft/tracker/progress.go
 * 
 * 1、维护follower节点的match、next索引,以便知道下一次从哪里开始同步数据
 * 2、维护着follower节点当前的状态
 * 3、同步快照数据的状态
 * 4、流量控制,避免follower节点超载
 * 
 * @author xuwenfeng
 * @author zhuam
 */
public class Progress {
	//
	// Next保存的是下一次leader发送append消息时传送过来的日志索引
	// 当选举出新的leader时,首先初始化Next为该leader最后一条日志+1
	// 如果向该节点append日志失败,则递减Next回退日志,一直回退到索引匹配为止

	// Match保存在该节点上保存的日志的最大索引,初始化为0
	// 正常情况下,Next = Match + 1
	// 以下情况下不是上面这种情况：
	// 1. 切换到Probe状态时,如果上一个状态是Snapshot状态,即正在接收快照,那么Next = max(pr.Match+1, pendingSnapshot+1)
	// 2. 当该follower不在Replicate状态时,说明不是正常的接收状态。
	//    此时当leader与follower同步leader上的日志时, 可能出现覆盖的情况, 即此时follower上面假设Match为3, 但是索引为3的数据会被
	//    leader覆盖,此时Next指针可能会一直回溯到与leader上日志匹配的位置,再开始正常同步日志,此时也会出现Next != Match + 1的情况出现
	//
	private volatile long matched;		
	private volatile long nextIdx;		
	private volatile ProgressState state;
	//
	private volatile boolean paused; // 是否暂停给这个follower发送 MsgAppend 
	private long pendingSnapshot; // 如果向该节点发送快照消息,pendingSnapshot用于保存快照消息的索引, 当 pendingSnapshot 不为0时,该节点也被标记为暂停状态(TODO: raft只有在这个正在进行中的快照同步失败以后,才会重传快照消息)
	//
	private volatile boolean recentActive;  // recentActive 标志位是leader节点在收到 follower的消息响应后被设置为true的
	private volatile long recentActiveTime;
	//
	private Inflights ins; // 用于实现滑动窗口,用来做流量控制
	private boolean isLearner; // 是否为 learner
	
	public Progress(long nextIdx, Inflights ins) {
		this(nextIdx, ins, false);
	}
	
	public Progress(long nextIdx, Inflights ins, boolean isLearner) {
		this.nextIdx = nextIdx;
		this.ins = ins;
		this.isLearner = isLearner;
		this.state = ProgressState.Probe;
	}
	//
	// 重置状态
	private void resetState(ProgressState state) {
		this.paused = false;
		this.pendingSnapshot = 0;
		this.state = state;
		this.ins.reset();
	}
	//
	// 修改为 Probe 状态
	public synchronized void becomeProbe() {
		// If the original state is ProgressState.Snapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1
		if ( this.state == ProgressState.Snapshot ) {
			 long pending_snapshot = this.pendingSnapshot;	 // 如果当前状态是接受快照状态,那么可以知道该节点已经成功接受处理了该快照,此时修改next索引需要根据max和快照索引来判断
			 this.resetState(ProgressState.Probe);
	         this.nextIdx = Math.max(this.matched + 1, pending_snapshot + 1);  // 取两者的最大值+1
		} else {
            this.resetState(ProgressState.Probe);
            this.nextIdx = this.matched + 1;
        }
	}
	
	public synchronized void becomeReplicate() {
		this.resetState(ProgressState.Replicate);
		this.nextIdx = this.matched + 1;
	}
	
	public synchronized void becomeSnapshot(long snapshotIndex) {
		this.resetState(ProgressState.Snapshot);
		this.pendingSnapshot = snapshotIndex;
	}
	
	public void snapshotFailure() {
		this.pendingSnapshot = 0;
	}
	
	/**
	 * 如果 matched 等于或高于pendingSnapshot,则可能会取消 pendingSnapshot
	 * 
	 * TODO: 当前为接收快照,同时match已经大于等于快照索引, 因为match已经大于快照索引了,所以这部分快照数据可以不接收了,也就是可以被中断的快照操作
	 *   因为在节点落后leader数据很多的情况下,可能leader会多次通过snapshot同步数据给节点,而当 pr.Match >= pr.PendingSnapshot的时候,
	 *   说明通过快照来同步数据的流程完成了,这时可以进入正常的接收同步数据状态了。
	 * @return
	 */
	public boolean maybeSnapshotAbort() {
		boolean isAbort = (this.state == ProgressState.Snapshot) && (this.matched >= this.pendingSnapshot);
		return isAbort;
	}

	/**
	 * 如果传入的n小于等于当前的match索引,则索引就不会更新,返回false；否则更新索引返回true。
	 * 
	 * TODO: 收到 append response 的成功应答之后,leader更新节点的索引数据
	 * @param n
	 * @return
	 */
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

	/**
	 * maybeDecrTo函数在传入的索引不在范围内的情况下返回false, 否则将把该节点的index减少到min(rejected,last)然后返回true
	 * @param rejected, rejected是拒绝该append消息时的索引
	 * @param last, last是拒绝该消息的节点的最后一条日志索引
	 * @return
	 */
	public synchronized boolean maybeDecrTo(long rejected, long last) {
		
		// 如果当前在接收状态
		if (this.state == ProgressState.Replicate) {
			// the rejection must be stale if the progress has matched and "rejected" is smaller than "match".
			if (rejected <= this.matched) {
				return false; // 这种情况说明返回的情况已经过期,中间有其他添加成功的情况,导致match索引递增,此时不需要回退索引,返回false
			}
			this.nextIdx = this.matched + 1; // 直接减少next到 match+1
			return true;
		}
		//
		// 以下都不是正常接收状态的情况
		//
		// the rejection must be stale if "rejected" does not match next - 1
		// 为什么这里不是对比Match？因为Next涉及到下一次给该Follower发送什么日志,
		// 所以这里对比和下面修改的是Next索引
		// Match只表示该节点上存放的最大日志索引,而当leader发生变化时,可能会覆盖一些日志
		//
		if (this.nextIdx == 0 || this.nextIdx - 1 != rejected) {
			// 这种情况说明返回的情况已经过期,不需要回退索引,返回false
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
	
	/**
	 * 一个节点当前处于暂停状态原因有几个：
	 * 		1)拒绝了最近的append消息(ProgressState.Probe状态)
	 * 		2)接受snapshot状态中(ProgressState.Snapshot状态)
	 *  	3)已经达到了需要限流的程度(ProgressState.Replicate状态且滑动窗口已满)
	 *  
	 *  isPaused在以下情况中返回true：
	 *  		1）等待接收快照 
	 *   		2) inflight数组已满,需要进行限流
	 *   		3) 进入ProgressState.Probe状态,这种状态说明最近拒绝了msgapps消息
	 *   
	 * @return
	 */
	public boolean isPaused() {
		switch(state) {
		case Probe:
			return paused;
		case Replicate:
			// 如果在replicate状态,pause与否,取决于infilght数组是否满了
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

	@Override
	public String toString() {
		return new StringBuffer() //
		.append("Progress [matched=").append(matched) //
		.append(", nextIdx=").append(nextIdx) //
		.append(", state=").append(state) //
		.append(", paused=").append(paused) //
		.append(", pendingSnapshot=").append(pendingSnapshot) //
		.append(", recentActive=").append(recentActive) //
		.append(", isLearner=").append(isLearner) //
		.append("]") //
		.toString(); //
	}
}