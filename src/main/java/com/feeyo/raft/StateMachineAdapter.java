package com.feeyo.raft;


public abstract class StateMachineAdapter implements StateMachine {
	protected long id;
	protected volatile long leaderId;
	protected volatile PeerSet peerSet;
	
	public StateMachineAdapter(long id, PeerSet peerSet) {
		this.id = id;
		this.peerSet = peerSet;
	}
	
	@Override
	public void initialize(long committed) {
		// ignored
	}

	@Override
	public void applyMemberChange(PeerSet peerSet, long committed) {
		this.peerSet = peerSet;
	}
	
	@Override
	public void leaderChange(long leaderId) {
		this.leaderId = leaderId;
	}
	
	@Override
	public void leaderCommitted(long leaderCommitted) {
		// ignored
	}
	
	//
	public long getId() {
		return id;
	}

	public long getLeaderId() {
		return leaderId;
	}
	
	public String getLeaderIp() {
		Peer peer = peerSet.get( leaderId );
		if ( peer != null )
			return peer.getIp();
		//
		return null;
	}

	public PeerSet getPeerSet() {
		return peerSet;
	}
	
	public boolean isLeader() {
		if ( id != 0 && leaderId != 0 && id == leaderId ) {
			return  true;
		}
		return false;
	}
	
	public boolean isFollower() {
		if ( id != 0 && leaderId != 0 && id != leaderId ) {
			return  true;
		}
		return false;
	}
	
}
