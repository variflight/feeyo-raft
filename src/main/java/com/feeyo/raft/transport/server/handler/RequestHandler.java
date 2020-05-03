package com.feeyo.raft.transport.server.handler;

import java.util.List;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.raft.transport.server.HttpConnection;

public interface RequestHandler {
	
	public static final String HTTP_GET = "GET";
	public static final String HTTP_POST = "POST";
	
	//
	public String getHttpMethod();
	public String getPath();

	//
	public void handle(HttpConnection conn, List<HttpRequest> requestList);

}
