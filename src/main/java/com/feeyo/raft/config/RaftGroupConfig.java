package com.feeyo.raft.config;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.raft.Config;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.group.proto.Raftgrouppb.Region;

public class RaftGroupConfig {
	
	// local is the identity of the local node
	private Peer local;

	// The peerSet of all nodes (including self) in the raft cluster. 
	// It should only be set when starting a new raft cluster.
	// Restarting raft from previous configuration will panic if peers is set.
	// peer is private and only used for testing right now.
	private PeerSet peerSet;
	
	//
	private int tpCoreThreads;
	private int tpMaxThreads;
	private int tpQueueCapacity;
	
	//
	private List<Region> regions;
	private Config cc;
	
	
	public RaftGroupConfig(Config c) {
		this.cc = c;
	}
	
	
	public Peer getLocal() {
		return local;
	}

	public void setLocalPeer(Peer local) {
		this.local = local;
		this.cc.setId( local.getId() );
	}
	
	public PeerSet getPeerSet() {
		return peerSet;
	}

	public void setPeerSet(PeerSet peerSet) {
		this.peerSet = peerSet;
		//
		List<Long> voters = new ArrayList<>();
		List<Long> learners = new ArrayList<>();
		//
		for (Peer peer : peerSet.values()) {
			if (peer.isLearner())
				learners.add(peer.getId());
			else
				voters.add(peer.getId());
		}
		this.cc.setVoters(voters);
		this.cc.setLearners(learners);
	}
	
	//
	//
	public int getTpCoreThreads() {
		return tpCoreThreads;
	}

	public void setTpCoreThreads(int tpCoreThreads) {
		this.tpCoreThreads = tpCoreThreads;
	}

	public int getTpMaxThreads() {
		return tpMaxThreads;
	}

	public void setTpMaxThreads(int tpMaxThreads) {
		this.tpMaxThreads = tpMaxThreads;
	}

	public int getTpQueueCapacity() {
		return tpQueueCapacity;
	}

	public void setTpQueueCapacity(int tpQueueCapacity) {
		this.tpQueueCapacity = tpQueueCapacity;
	}
	
	//
	// Region & Config
	//
	public List<Region> getRegions() {
		return regions;
	}

	public void setRegions(List<Region> regions) {
		this.regions = regions;
	}

	public Config getCc() {
		return cc;
	}
}
