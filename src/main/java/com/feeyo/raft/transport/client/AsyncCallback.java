package com.feeyo.raft.transport.client;

import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.http.HttpResponseDecoder;
import com.feeyo.net.nio.NetSystem;

/**
 * 
 * @author zhuam
 *
 */
public class AsyncCallback implements ConCallback {
	//
	private AsyncHttpResponseHandler responseHandler;
	//
	private HttpResponseDecoder responseDecoder = new HttpResponseDecoder();
	//
	public AsyncCallback(AsyncHttpResponseHandler handler) {
		this.responseHandler = handler;
	}

	@Override
	public void connectionAcquired(HttpClientConnection conn) {
		// ignore
	}
	
	@Override
	public void connectionError(HttpClientConnection conn, Exception ex) {
		failed( ExceptionUtils.getStackTrace(ex) );
	}

	@Override
	public void handleResponse(final HttpClientConnection conn, final byte[] data) throws IOException {
		final HttpResponse response = responseDecoder.decode(data);
		if ( response != null ) {
			//
			if (responseHandler != null)  {
				// 此处需要走业务线程池调度，尽快脱离IO线程池
				NetSystem.getInstance().getBusinessExecutor().execute(new Runnable() {
					@Override
					public void run() {
						responseHandler.completed( response );
					}
				});
			}
			// 释放连接
			conn.release();
		}
	}

	@Override
	public void connectionClose(HttpClientConnection conn, String reason) {
		failed( reason );
	}
	//
	private void failed(final String reason) {
		if ( responseHandler != null ) {
			NetSystem.getInstance().getBusinessExecutor().execute(new Runnable() {
				@Override
				public void run() {
					responseHandler.failed(reason);
				}
			});
		}
	}
	
}