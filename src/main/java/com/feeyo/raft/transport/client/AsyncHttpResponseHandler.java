package com.feeyo.raft.transport.client;

import com.feeyo.net.codec.http.HttpResponse;

public interface AsyncHttpResponseHandler {
	//
	void completed(HttpResponse response);
	//
	void failed(String reason);
}
