package com.feeyo.raft.test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.test.VirtualNode.SyncWaitCallback;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;

public class VirtualRaftCluster {
	
	// RaftNode ...
	public static Map<Long, VirtualNode> nodeSet = new ConcurrentHashMap<>();
	
	public static volatile long leaderId = -1;
	
	public VirtualRaftCluster() throws RaftException {
		
		Peer peer1 = new Peer(1, "127.0.0.1", 8081, false, 160);
		Peer peer2 = new Peer(2, "127.0.0.1", 8082, false, 70);
		Peer peer3 = new Peer(3, "127.0.0.1", 8083, false, 40);
		
		PeerSet peerSet = new PeerSet();
		peerSet.put( peer1 );
		peerSet.put( peer2 );
		peerSet.put( peer3 );
		//
		VirtualNode n0 = new VirtualNode( peer1, peerSet);
		VirtualNode n1 = new VirtualNode( peer2, peerSet);
		VirtualNode n2 = new VirtualNode( peer3, peerSet);
		//
		n0.start();
		n1.start();
		n2.start();
		
	}
	
	
	public void syncWait(Message message, SyncWaitCallback c) {
		if ( leaderId != -1 ) {
			VirtualNode node = nodeSet.get(leaderId);
			if ( node != null )
				node.syncWait(message, c);
		} else  {
			///
			System.out.println( leaderId );
		}
	}

}
