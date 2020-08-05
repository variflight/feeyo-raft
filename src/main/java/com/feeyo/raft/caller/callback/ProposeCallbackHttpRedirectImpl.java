package com.feeyo.raft.caller.callback;

import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.feeyo.util.internal.Utf8Util;

//
// 用于 follower 跳转
public class ProposeCallbackHttpRedirectImpl extends ProposeCallback {
	//
	private HttpConnection conn;

	public ProposeCallbackHttpRedirectImpl(String key, HttpConnection conn, long createMs) {
		super(key, createMs);
		this.conn = conn;
	}
	
	public void onCompleted() {
		super.onCompleted();
		HttpUtil.sendOk(conn);
	}
	
	public void onFailed(byte[] errMsg) {
		super.onFailed(errMsg);
		HttpUtil.sendError(conn, errMsg == null ? "raft failed!": Utf8Util.readUtf8(errMsg));
	}
}
