package com.feeyo.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftNodeAdapter implements RaftNodeListener {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftNodeAdapter.class );
	//
	protected volatile int targetPriority;			// 目标leader 的选举权重值
	protected volatile int electionTimeoutCounter;	// 当前节点的选举超时数
	
	//
	@Override
	public boolean isAllowElection() {
		//
		// Priority 0 is a special value so that a node will never participate in election.
		Peer local = this.getPeer();
        if (local.isPriorityNotElected()) 
            return false;

        // If this nodes disable priority election, then it can make a election.
        if (local.isPriorityDisabled()) 
            return true;
        //
        // If current node's priority < target_priority, it does not initiate leader,
        // election and waits for the next election timeout.
        if (local.getPriority() < this.targetPriority) {
            this.electionTimeoutCounter++;
            //
            // If next leader is not elected until next election timeout, it
            // decays its local target priority exponentially.
            if (this.electionTimeoutCounter > 1) {
                decayTargetPriority();
                this.electionTimeoutCounter = 0;
            }

            if (this.electionTimeoutCounter == 1) 
                return false;
        }

        return local.getPriority() >= this.targetPriority;
	}
	
	@Override
	public int getNodeTargetPriority() {
		return this.targetPriority;
	}
	
	@Override
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
        // Default Gap value should be bigger than 10.
        final int decayPriorityGap = 10;
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));

        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        LOGGER.info("Node {} priority decay, from: {}, to: {}.", this.getPeer().getId(), prevTargetPriority, this.targetPriority);
    }

}
