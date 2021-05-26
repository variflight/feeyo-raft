package com.feeyo.raft.transport.server.handler;

import java.util.List;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpResponseStatus;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;

public class RaftGroupCliRequestHandler implements RequestHandler {
	//
	private RaftGroupServer raftGroupSrv = null;
	
	public RaftGroupCliRequestHandler(RaftGroupServer raftGroupSrv) {
		this.raftGroupSrv = raftGroupSrv;
	}
	
	@Override
	public String getHttpMethod() {
		return HTTP_POST;
	}

	@Override
	public String getPath() {
		return "/raft/group/cli";
	}
	
	@Override
	public void handle(HttpConnection conn, List<HttpRequest> requestList) {
		
		if ( raftGroupSrv == null )
			HttpUtil.send(conn, HttpResponseStatus.BAD_REQUEST);
		
		//
	}

}
