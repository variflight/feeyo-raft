package com.feeyo.raft.config;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.raft.Config;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;

//
public class RaftConfig {
	//
	// local is the identity of the local node
	private Peer local;
	
	// The peerSet of all nodes (including self) in the raft cluster. 
	// It should only be set when starting a new raft cluster.
	private PeerSet peerSet;
	
	// Configuration of raft
	private final Config cfg;
	//
	private int tpCoreThreads;
	private int tpMaxThreads;
	private int tpQueueCapacity;
	
	//
	public RaftConfig(Config cfg) {
		this.cfg = cfg;
	}

	public Peer getLocal() {
		return local;
	}

	public void setLocalPeer(Peer local) {
		this.local = local;
		this.cfg.setId(local.getId());
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
		this.cfg.setVoters(voters);
		this.cfg.setLearners(learners);
	}
	//
	// raft需要的配置
	public Config getCfg() {
		return cfg;
	}
	
	///
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
}