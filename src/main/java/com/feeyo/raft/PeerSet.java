package com.feeyo.raft;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerSet {
	
	private Map<Long, Peer> peers = new ConcurrentHashMap<Long, Peer>();
	
	public void put(Peer peer) {
		peers.put(peer.getId(), peer);
	}
	
	public Peer remove(long id) {
		return peers.remove(id);
	}
	
	public Peer get(long id) {
		return peers.get(id);
	}
	
	
	public Collection<Peer> values() {
		return peers.values();
	}
	
	//
	public boolean isExist(long id, String ip, int port) {
		for (Peer peer : peers.values()) {
			if (peer.getId() == id) 
				return true;
			//
			if (peer.getIp().equals(ip) && peer.getPort() == port) 
				return true;
		}
		return false;
	}
	
}