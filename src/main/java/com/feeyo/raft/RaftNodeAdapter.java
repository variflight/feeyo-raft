package com.feeyo.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftNodeAdapter implements RaftNodeListener {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( RaftNodeAdapter.class );
	
	// If next leader is not elected until next election timeout, it exponentially
    // decay its local target priority, for example target_priority = target_priority - gap
    // Default: 10
	private static final int decayPriorityGap = 10;
	//
	protected volatile int targetPriority;				// 目标leader 的选举权重值
	protected volatile int electionTimeoutCounter;		// 当前节点的选举超时数
	
	//
	public abstract Peer getPeer();
	public abstract PeerSet getPeerSet();
	
	/*
	 * 是否允许通过比较节点的优先级和目标优先级来启动选举。
	 * 同时，如果下一任领导人直到下一次选举超时才当选，那么它的本地目标优先级就会呈指数衰减
	 */
	@Override
	public boolean isAllowLaunchElection() {
		//
		// Priority 0，节点永远不会参与选举
		Peer local = this.getPeer();
        if (local.isPriorityNotElected()) {
        	LOGGER.info("self={}, priority:{}, not elected!", getPeer().getId(), local.getPriority());
            return false;
        }
        //
        // Priority <= -1, 如果此节点禁用优先级选择，返回进行选择
        if (local.isPriorityDisabled()) {
            return true;
        }
        //
        LOGGER.info("self={}, priority:{}, target_priority:{}", getPeer().getId(), local.getPriority(), this.targetPriority);
        //
        // 当前节点 priority值 < 本地全局变量 targetPriority值, 执行目标优先级衰减降级并等待下次选举超时
        if (local.getPriority() < this.targetPriority) {
            this.electionTimeoutCounter++;
            if (this.electionTimeoutCounter > 1) {
                decayTargetPriority();
                this.electionTimeoutCounter = 0;
            }
            //
            if (this.electionTimeoutCounter == 1) 
                return false;
        }
        return local.getPriority() >= this.targetPriority;
	}
	
	public int getNodeTargetPriority() {
		return this.targetPriority;
	}
	
	//
	// 获取同一Raft组中所有节点的最大优先级值，并更新当前节点的目标优先级值
	public int getMaxPriorityOfNodes() {
		int maxPriority = Integer.MIN_VALUE;
		for (final Peer peer : this.getPeerSet().values()) {
			final int priorityVal = peer.getPriority();
			maxPriority = Math.max(priorityVal, maxPriority);
		}
		return maxPriority;
	}
	
	//
	// 基于间隙值的衰减目标优先级值
    private void decayTargetPriority() {
        // 全局变量 targetPriority 值进行 20% 的衰减
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));
        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        //
        LOGGER.info("self={}, priority decay, from:{}, to:{}.", this.getPeer().getId(), prevTargetPriority, this.targetPriority);
    }
    
}