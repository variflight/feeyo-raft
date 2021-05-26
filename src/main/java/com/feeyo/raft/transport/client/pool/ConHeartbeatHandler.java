package com.feeyo.raft.transport.client.pool;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.primitives.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpRequestEncoder;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.transport.client.ConCallback;
import com.feeyo.raft.transport.client.HttpClientConnection;

/**
 * check connection
 * 
 * @author zhuam
 */
public class ConHeartbeatHandler implements ConCallback {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( ConHeartbeatHandler.class );
	
	private static final byte[] RESPONSE_OK_BYTES = "HTTP/1.1 200 OK".getBytes();
	private static final HttpRequestEncoder httpRequestEncoder = new HttpRequestEncoder();
	//
	private final ConcurrentHashMap<Long, HeartbeatCon> allCons = new ConcurrentHashMap<>();
	
	public void doHeartbeat(HttpClientConnection conn) {
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("do heartbeat check for con " + conn);
		}
		
		try {
			HeartbeatCon hbCon = new HeartbeatCon(conn);
			boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
			if (notExist) {				
				conn.setCallback( this );
				
				// fix
				HttpRequest request = new HttpRequest("GET", "/raft/ping");
				request.addHeader("Content-Length", "0");
				//
				byte[] buffer = httpRequestEncoder.encode(request);
				conn.write( buffer );
			}
		} catch (Exception e) {
			executeException(conn, e);
		}
	}
	
	/**
	 * remove timeout connections
	 */
	public void abandTimeoutConns() {
		
		if (allCons.isEmpty()) {
			return;
		}
		
		Collection<HttpClientConnection> abandCons = new LinkedList<>();
		long curTime = TimeUtil.currentTimeMillis();
		Iterator<Entry<Long, HeartbeatCon>> itors = allCons.entrySet().iterator();
		while (itors.hasNext()) {
			HeartbeatCon chkCon = itors.next().getValue();
			if (chkCon.timeoutTimestamp < curTime) {
				abandCons.add(chkCon.conn);
				itors.remove();
			}
		}

		if (!abandCons.isEmpty()) {
			for (HttpClientConnection con : abandCons) {
				try {
					// if(con.isBorrowed())
					con.close("heartbeat check, backend conn is timeout !!! ");
				} catch (Exception e) {
					LOGGER.error("close err:", e);
				}
			}
		}
		abandCons.clear();
	}
	
	private void removeFinished(HttpClientConnection con) {
		Long id = con.getId();
		this.allCons.remove(id);
	}
	
	private void executeException(HttpClientConnection c, Throwable e) {
		removeFinished(c);
		LOGGER.error("executeException: ", e);
		c.close("heartbeat exception:" + e);
	}

	@Override
	public void connectionError(HttpClientConnection conn, Exception e) {
		// not called
	}

	@Override
	public void connectionAcquired(HttpClientConnection conn) {
		// not called
	}

	@Override
	public void handleResponse(HttpClientConnection conn, byte[] buffer) throws IOException {
		//
		removeFinished(conn);
		//
		if ( Bytes.indexOf(buffer, RESPONSE_OK_BYTES) > -1 ) {
			conn.release();	

		} else {
			conn.close("heartbeat err");
		}
	}
	
	@Override
	public void connectionClose(HttpClientConnection conn, String reason) {
		removeFinished(conn);		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("connection closed " + conn + " reason:" + reason);
		}
	}
}


class HeartbeatCon {
	public final long timeoutTimestamp;
	public final HttpClientConnection conn;

	public HeartbeatCon(HttpClientConnection conn) {
		super();
		this.timeoutTimestamp = TimeUtil.currentTimeMillis() + ( 20 * 1000L );
		this.conn = conn;
	}
}
