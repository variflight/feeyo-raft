package com.feeyo.raft.transport.client;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;
import com.feeyo.net.nio.util.TimeUtil;

public class HttpClientConnection extends Connection {
	
	private static final long TIMEOUT =  60 * 1000L;

	private ConCallback callback;
	private PhysicalNode physicalNode;
	private volatile boolean borrowed = false;
	private volatile long lastTime;

	public HttpClientConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}

	//
	public ConCallback getCallback() {
		return callback;
	}

	public void setCallback(ConCallback callback) {
		this.callback = callback;
	}
	
	public PhysicalNode getPhysicalNode() {
		return physicalNode;
	}

	public void setPhysicalNode(PhysicalNode node) {
		this.physicalNode = node;
	}

	public void release() {
		this.setBorrowed(false);
		//
		if ( this.physicalNode != null )
			this.physicalNode.releaseConnection(this);
	}

	public void setBorrowed(boolean borrowed) {
		this.borrowed = borrowed;
	}

	public boolean isBorrowed() {
		return this.borrowed;
	}

	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;
	}
	
	@Override
	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + TIMEOUT;
	}
	
	@Override
	public String toString() {
		return new StringBuffer(100).append( "[" )
		.append(" host=").append( host ).append(":").append( port )
		.append(", id=").append( id )
		.append(", startT=").append( startupTime )
		.append(", lastWT=").append( lastWriteTime )
		.append(", state=").append( state )
		.append(", isClosed=").append( isClosed )
		.append("]")
		.toString();
	}

}