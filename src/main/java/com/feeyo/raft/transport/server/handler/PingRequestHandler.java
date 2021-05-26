package com.feeyo.raft.transport.server.handler;

import java.util.List;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;

public class PingRequestHandler implements RequestHandler {
	
	@Override
	public String getHttpMethod() {
		return HTTP_GET;
	}

	@Override
	public String getPath() {
		return "/raft/ping";
	}

	@Override
	public void handle(HttpConnection conn, List<HttpRequest> requestList) {
		HttpUtil.sendOk(conn);
	}

}
