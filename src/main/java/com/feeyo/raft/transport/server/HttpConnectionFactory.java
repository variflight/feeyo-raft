package com.feeyo.raft.transport.server;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.ConnectionFactory;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.RaftServer;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.transport.server.handler.RaftCliRequestHandler;
import com.feeyo.raft.transport.server.handler.RaftGroupCliRequestHandler;
import com.feeyo.raft.transport.server.handler.RaftGroupMessageRequestHandler;
import com.feeyo.raft.transport.server.handler.PingRequestHandler;
import com.feeyo.raft.transport.server.handler.RaftMessageRequestHandler;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Raft Transport
 * 
 * @author zhuam
 *
 */
public class HttpConnectionFactory extends ConnectionFactory {
	
	private RaftGroupServer raftGroupSrv = null;
	private RaftServer raftSrv = null;
	
	public HttpConnectionFactory(RaftServer raftServer) {
		this.raftSrv = raftServer;
		this.registerRequestHandlers();
	}
	
	public HttpConnectionFactory(RaftGroupServer raftGroupServer) {
		this.raftGroupSrv = raftGroupServer;
		this.registerRequestHandlers();
	}
	
	private void registerRequestHandlers() {
		//
		// Raft
		HttpConnectionHandler.registerHandler( new RaftMessageRequestHandler( raftSrv ) );
		HttpConnectionHandler.registerHandler( new RaftCliRequestHandler( raftSrv ) );
		//
		// Mulit-raft
		HttpConnectionHandler.registerHandler( new RaftGroupMessageRequestHandler( raftGroupSrv ) );		
		HttpConnectionHandler.registerHandler( new RaftGroupCliRequestHandler( raftGroupSrv ) );		
		HttpConnectionHandler.registerHandler( new PingRequestHandler() );
	}

	@Override
	public ClosableConnection make(SocketChannel channel) throws IOException {
		//
		HttpConnectionHandler connectionHandler = new HttpConnectionHandler();
		HttpConnection c = new HttpConnection(channel);
		NetSystem.getInstance().setSocketParams(c);	// 设置连接的参数
        c.setHandler( connectionHandler );	// 设置NIOHandler
        c.setIdleTimeout( NetSystem.getInstance().getNetConfig().getIdleTimeout() );
		return c;
	}

}
