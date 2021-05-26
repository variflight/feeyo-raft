package com.feeyo.raft.transport;

import java.io.IOException;

import com.feeyo.net.nio.NIOAcceptor;
import com.feeyo.net.nio.NIOReactorPool;
import com.feeyo.raft.RaftServer;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.transport.server.HttpConnectionFactory;

public class TransportServer {
	
	// NIO
	private NIOReactorPool nioReactorPool = null;
	private NIOAcceptor nioAcceptor = null;
	
	private boolean isGroup = false;
	//
	private RaftServer raftSrv;
	private RaftGroupServer raftGroupSrv;
	
	public TransportServer(RaftServer raftSrv) {
		this.raftSrv = raftSrv;
		this.isGroup = false;
	}
	
	public TransportServer(RaftGroupServer raftGroupSrv) {
		this.raftGroupSrv = raftGroupSrv;
		this.isGroup = true;
	}
	
	//
	public void start(int port, int reactorSize) throws IOException {
		this.nioReactorPool = new NIOReactorPool("rf/trans/reactor", reactorSize);
		this.nioAcceptor = new NIOAcceptor("rf/trans/rpc", "0.0.0.0", port, 										//
					isGroup ? new HttpConnectionFactory(raftGroupSrv) : new HttpConnectionFactory(raftSrv), 		//
							     nioReactorPool);																	//
		this.nioAcceptor.start();
		
	}
	
	public void stop() {	
		if( this.nioAcceptor != null) {
			this.nioAcceptor.interrupt();
			this.nioAcceptor = null;
		}
	}
}