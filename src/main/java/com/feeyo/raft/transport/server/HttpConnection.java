package com.feeyo.raft.transport.server;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;
import com.feeyo.net.nio.util.TimeUtil;

public class HttpConnection extends Connection {
	
	private static final long TIMEOUT = 5 * 60 * 1000L;
	
	public HttpConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}
	
	
	@Override
	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + TIMEOUT;
	}
	
	@Override
	public String toString() {
		
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Conn[" );
		sbuffer.append("reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append(":").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", startup=").append( startupTime );
		sbuffer.append(", lastRT=").append( lastReadTime );
		sbuffer.append(", lastWT=").append( lastWriteTime );
		sbuffer.append(", attempts=").append( writeAttempts );	
		sbuffer.append(", isClosed=").append( isClosed );
		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
}
