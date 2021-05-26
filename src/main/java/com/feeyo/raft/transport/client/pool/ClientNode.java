package com.feeyo.raft.transport.client.pool;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.transport.client.HttpClientConnection;
import com.feeyo.raft.transport.client.HttpClientConnectionFactory;
import com.feeyo.raft.transport.client.PhysicalNode;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.util.internal.Utf8Util;
import com.google.common.base.Joiner;

public class ClientNode {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( ClientNode.class );
	//
	private static ConHeartbeatHandler conHeartbeatHandler = new ConHeartbeatHandler();
	
	private long id;
	private String host;
	private int port;
	private int minCon;
	private int maxCon;
	
	private HttpClientConnectionFactory connectionFactory = null;
	private PhysicalNode physicalNode = null;
	private volatile int heartbeatRetry = 0;
	private volatile int heartbeatStatus = 1;
	
	public ClientNode(HttpClientConnectionFactory factory, long id, String host, int port, int minCon, int maxCon) {
		this.connectionFactory = factory;
		this.id = id;
		this.host = host;
		this.port = port;
		this.minCon = minCon;
		this.maxCon = maxCon;
		//
		physicalNode = new PhysicalNode(connectionFactory, id, host, port, minCon, maxCon);
		physicalNode.initConnections();		
	}
	
	public void stop() {
		physicalNode.clearConnections("forced closure!");
		physicalNode = null;
	}
	
	//
	public PhysicalNode getPhysicalNode() {
		if ( heartbeatStatus == -1 )
			return null;
		return physicalNode;
	}

	//
	public void heartbeatCheck(long heartbeatTime, long closeTime) {	
		if ( heartbeatStatus == -1 )
			return;
		//
		List<HttpClientConnection> heartbeatCons = physicalNode.getNeedHeartbeatCons(heartbeatTime, closeTime);			
		for (HttpClientConnection conn : heartbeatCons) {
			conHeartbeatHandler.doHeartbeat( conn );
		}
		heartbeatCons.clear();		
		conHeartbeatHandler.abandTimeoutConns();
		
		// 连接池 动态调整逻辑
		// -------------------------------------------------------------------------------
//		if ( LOGGER.isDebugEnabled() ) {
//			LOGGER.debug( "heartbeat check: id={}, time={}",  id, TimeUtil.currentTimeMillis() );
//		}
		
		int idleCons = physicalNode.getIdleCount();
		int activeCons = physicalNode.getActiveCount();
		if (idleCons > minCon) {
			if (idleCons < activeCons)
				return;
			// 闲置太多
			physicalNode.closeByIdleMany(idleCons - minCon);

		} else if (idleCons < minCon) {
			if (idleCons > (minCon * 0.5))
				return;
			//闲置太少
			if ((idleCons + activeCons) < maxCon) {
				int createCount = (int) Math.ceil((minCon - idleCons) / 3F);
				physicalNode.createByIdleLitte(idleCons, createCount);
			}			
		}
	}

	public void availableCheck() {
		boolean isOk = httpGet(host, port, "/raft/ping");
		if (isOk) {
			if (heartbeatStatus == -1) {
				LOGGER.warn("Node {} network ok!", this.id);
			}
			heartbeatRetry = 0;
			heartbeatStatus = 1;
		} else {
			if (++heartbeatRetry == 5) 
				heartbeatStatus = -1;
		}
		
		if (heartbeatStatus == -1) {
			LOGGER.warn("Node {} network error, pls check!", this.id);
			physicalNode.clearConnections("network error");
		}
	}
	
	//
	private static Joiner requestJoiner = Joiner.on("-");
	private static Map<String, byte[]> requestBytesCache = new ConcurrentHashMap<>();
	//
	private static ThreadLocal<byte[]> responseBytesGen = new ThreadLocal<byte[]>() {
		protected byte[] initialValue() {
	        return new byte[1024];
	    }
	};
	
	//
	private boolean httpGet(String host, int port, String path) {
		boolean isOk = false;
		String key = requestJoiner.join(host, port, path);
		byte[] requestBytes = requestBytesCache.get(key);
		if ( requestBytes == null ) {
			StringBuffer strBuffer = new StringBuffer(250); 
			strBuffer.append("GET ").append( path ).append(" HTTP/1.1").append( "\r\n" );	
			strBuffer.append("Host: ").append( host ).append( "\r\n" );
			strBuffer.append("Content-Length: ").append( 0 ).append( "\r\n" );
			strBuffer.append( "\r\n" );
			requestBytes = strBuffer.toString().getBytes();
			requestBytesCache.put(key, requestBytes);
		}
		//
		Socket socket = null;
		OutputStream out = null;
		InputStream in = null;
		try {
			socket = new Socket(host, port);
			socket.setSoTimeout(3000);
			out = socket.getOutputStream();
			out.write( requestBytes ); 							
			out.flush();
			//
			// reset
			byte[] buf = responseBytesGen.get();
			for(int i = 0; i < buf.length; i++)
				buf[i] = 0;
			//
			// receive
			in = socket.getInputStream();
			int len = in.read(buf);
			if ( len > 0 ) {
				String result = Utf8Util.readUtf8(buf, 0, len);
				int indexOf = result.indexOf("HTTP/1.1 200 OK");
				if (indexOf > -1) {
					isOk = true;
				}
			}
		} catch (Throwable e) {
			isOk = false;
		} finally {
			IOUtil.closeQuietly(out);
			IOUtil.closeQuietly(in);
			IOUtil.closeQuietly(socket);
		}
		return isOk;
	}
}