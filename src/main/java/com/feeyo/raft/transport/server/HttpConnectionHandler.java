package com.feeyo.raft.transport.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.UnknownProtocolException;
import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpRequestPipeliningDecoder;
import com.feeyo.net.codec.http.PathTrie;
import com.feeyo.net.codec.http.UriUtil;
import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.transport.server.handler.RequestHandler;
import com.feeyo.raft.transport.server.util.HttpUtil;


public class HttpConnectionHandler implements NIOHandler<HttpConnection>{
	
	private static Logger LOGGER = LoggerFactory.getLogger( HttpConnectionHandler.class );
	
	private HttpRequestPipeliningDecoder requestDecoder = new HttpRequestPipeliningDecoder();
		
	///
	private final static PathTrie<RequestHandler> getHandlers = new PathTrie<>();
	private final static PathTrie<RequestHandler> postHandlers = new PathTrie<>();
	
	public static void registerHandler(RequestHandler handler) {
		String method = handler.getHttpMethod();
		if (method.equalsIgnoreCase( RequestHandler.HTTP_GET )) {
			getHandlers.insert(handler.getPath(), handler);
			
		} else if ( method.equalsIgnoreCase( RequestHandler.HTTP_POST )) {
			postHandlers.insert(handler.getPath(), handler);
			
		} else {
			throw new RuntimeException("HttpMethod is not supported");
		}
	}
	
	//
	public static RequestHandler getHandler(String method, String uri) {
		// 获取 Path & Parameters
		String path = UriUtil.parsePath(uri);
		Map<String, String> parameters = UriUtil.parseParameters(uri);
		RequestHandler handler = null;
		
		if (method.equals( RequestHandler.HTTP_GET )) {
			handler = getHandlers.retrieve(path, parameters);

		} else if (method.equals( RequestHandler.HTTP_POST )) {
			handler = postHandlers.retrieve(path, parameters);
		}
		return handler;
	}


	private long t1 = 0;
	private int c = 0;
	private int s = 0;


	@Override
	public void handleReadEvent(final HttpConnection conn, byte[] data) throws IOException {
		
		// if ( LOGGER.isDebugEnabled() )
		// 		LOGGER.debug("recv: host={}, dataSize={}", conn.getHost(), data.length);
			
		try {
			
			if ( t1 == 0 )  {
				t1 = TimeUtil.currentTimeMillis();
			}
			c++;
			s += data.length;
			
			final List<HttpRequest> requestList = requestDecoder.decode(data);
			
			long now = TimeUtil.currentTimeMillis();
			if ( now - t1 > 60000 && c > 1 && requestList == null ) {
				
				int bufSize = requestDecoder.getBufferSize();
				int readOffset = requestDecoder.getReadOffset();
				LOGGER.error("decode err: host={}, bufSize={} readOffset={} ", conn.getHost(), bufSize, readOffset);
				//
				// java.io.File file = new java.io.File("/home/zhuaming/newdb/logs/" + dataSize + "-" + readOffset + ".txt");
				// com.google.common.io.Files.write( requestDecoder.getData(), file);
				//
				throw new IOException("http codec err!");
			}
			//
			
			if ( requestList != null ) {
				
				long t2 = TimeUtil.currentTimeMillis();
				long diff = t2 - t1;
				if( diff > 50 ) {
					LOGGER.info("http request codec slow, c={}, s={}, time={}", c, s, diff);
				}
				
				t1 = 0;
				c = 0;
				s = 0;
				
				//
				HttpRequest firstRequest = requestList.get(0);
				final RequestHandler handler = getHandler(firstRequest.getMethod(), firstRequest.getUri());
				if ( handler != null ) {
					try {
						// 由于raft 内部缠绕较多，此处启动 Boss 线程池，使其尽快脱离Network/IO
						NetSystem.getInstance().getBusinessExecutor().execute(new Runnable(){
							@Override
							public void run() {
								handler.handle(conn, requestList);
							}
						});
						
					} catch (RejectedExecutionException e) {	
						//LOGGER.error("handleReadEvent, reject execution err:", e);
						HttpUtil.sendError(conn, ExceptionUtils.getStackTrace(e));
					}
	
				} else {
					HttpUtil.sendError(conn, "No handler!");
					LOGGER.error("no handler, {}", firstRequest.toString());
				}
			}
			
		} catch(UnknownProtocolException e) {
			//
			LOGGER.warn( "Unknown protocol err:  ex={}, host={}, port={}",
					ExceptionUtils.getStackTrace(e), conn.getHost(), conn.getPort() );
			//
			throw new IOException("http protocol err!");
		}
		
	}
	
	@Override
	public void onConnected(HttpConnection conn) throws IOException {
		// LOGGER.debug("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(HttpConnection conn, Exception e) {
		// LOGGER.debug("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(HttpConnection conn, String reason) {
		if ( reason != null && !reason.equals("stream closed"))
			LOGGER.warn("onClosed(): {}, {}", conn, reason);
	}
}