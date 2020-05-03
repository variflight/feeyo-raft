package com.feeyo.raft.group;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;

//
public abstract class RaftGroupStateMachineAdapter implements RaftGroupStateMachine {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftGroupStateMachineAdapter.class );
	
	protected long id;
	//
	protected Map<Long, Long> regionLeaders = new ConcurrentHashMap<>();
	//
	protected PeerSet peerSet;
	
	public RaftGroupStateMachineAdapter(long id, PeerSet peerSet) {
		this.id = id;
		this.peerSet = peerSet;
	}
	
	@Override
	public void initialize(long regionId, long committed) {
		// ignored
	}


	@Override
	public void applyMemberChange(long regionId, PeerSet peerSet, long committed) {
		this.peerSet = peerSet;
	}

	@Override
	public void leaderChange(long regionId, long leaderId) {
		this.regionLeaders.put(regionId, leaderId);
		//
		LOGGER.debug("leaderChange region={} leaderId={}", regionId, leaderId);
	}

	@Override
	public void leaderCommitted(long regionId, long leaderCommitted) {
		// ignored
	}
	
	//
	public long getId() {
		return id;
	}

	public long getLeaderId(long regionId) {
		Long leaderId = regionLeaders.get( regionId );
		if ( leaderId == null )
			return 0;
		return leaderId;		
	}
	
	public String getLeaderIp(long regionId) {
		long leaderId = getLeaderId( regionId );
		Peer peer = peerSet.get( leaderId );
		if ( peer != null )
			return peer.getIp();
		//
		return null;
	}
	
	public boolean isLeader(long regionId) {
		long leaderId = getLeaderId( regionId );
		if ( id != 0 && leaderId != 0 && id == leaderId ) 
			return  true;
		return false;
	}
	
	public boolean isFollower(long regionId) {
		long leaderId = getLeaderId( regionId );
		if ( id != 0 && leaderId != 0 && id != leaderId ) {
			return  true;
		}
		return false;
	}
	
	//
	public PeerSet getPeerSet() {
		return peerSet;
	}
}
