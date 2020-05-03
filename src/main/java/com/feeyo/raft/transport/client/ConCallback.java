package com.feeyo.raft.transport.client;

import java.io.IOException;

public interface ConCallback {

	// 已获得有效连接的响应处理
	void connectionAcquired(HttpClientConnection conn);

	// 无法获取连接
	void connectionError(HttpClientConnection conn, Exception e);

	// 收到数据包的响应处理
	void handleResponse(HttpClientConnection conn, byte[] data) throws IOException;

	// 连接关闭
	void connectionClose(HttpClientConnection conn, String reason);

}
