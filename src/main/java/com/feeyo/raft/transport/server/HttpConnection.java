package com.feeyo.raft.transport.server;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;
import com.feeyo.net.nio.util.TimeUtil;

public class HttpConnection extends Connection {
	
	private static final long TIMEOUT = 5 * 60 * 1000L;
	
	public HttpConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}
	//
	@Override
	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + TIMEOUT;
	}
	
	@Override
	public String toString() {
		return new StringBuffer(100)
		.append( "Conn[" )
		.append("reactor=").append( reactor )
		.append(", host=").append( host ).append(":").append( port )
		.append(", id=").append( id )
		.append(", startup=").append( startupTime )
		.append(", lastRT=").append( lastReadTime )
		.append(", lastWT=").append( lastWriteTime )
		.append(", attempts=").append( writeAttempts )
		.append(", isClosed=").append( isClosed )
		.append("]")
		.toString();
	}
}