package com.feeyo.raft.transport.client;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NIOHandler;

public class HttpClientConnectionHandler implements NIOHandler<HttpClientConnection> {
	
	protected static Logger LOGGER = LoggerFactory.getLogger(HttpClientConnectionHandler.class);

	@Override
	public void onConnected(HttpClientConnection con) throws IOException {
		ConCallback callback = con.getCallback();
		if (callback != null)
			callback.connectionAcquired(con);
	}

	@Override
	public void onConnectFailed(HttpClientConnection con, Exception e) {
		ConCallback callback = con.getCallback();
		if (callback != null)
			callback.connectionError(con, e);
	}

	@Override
	public void onClosed(HttpClientConnection con, String reason) {
		ConCallback callback = con.getCallback();
		if (callback != null)
			callback.connectionClose(con, reason);
	}

	@Override
	public void handleReadEvent(HttpClientConnection con, byte[] data) throws IOException {
		ConCallback callback = con.getCallback();
		if (callback != null)
			callback.handleResponse(con, data);
		return;
	}
}