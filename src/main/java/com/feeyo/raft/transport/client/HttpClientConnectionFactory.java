package com.feeyo.raft.transport.client;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.NIOConnector;
import com.feeyo.net.nio.NetSystem;

public class HttpClientConnectionFactory {
	
	private NIOConnector nioConnector;
	
	public HttpClientConnectionFactory(NIOConnector nioConnector) {
		this.nioConnector = nioConnector;
	}
	
	public HttpClientConnection make(PhysicalNode physicalNode, ConCallback callback) throws IOException {
		String host = physicalNode.getHost();
		int port = physicalNode.getPort();
		
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);
		HttpClientConnection c = new HttpClientConnection( channel );
		NetSystem.getInstance().setSocketParams(c);
		//
		// 设置NIOHandlers
		c.setHandler(new HttpClientConnectionHandler());
		c.setHost(host);
		c.setPort(port);
		c.setPhysicalNode(physicalNode);
		c.setCallback(callback);
		c.setIdleTimeout(NetSystem.getInstance().getNetConfig().getIdleTimeout());
		// 连接 
		nioConnector.postConnect(c);
		return c;
	}

}
