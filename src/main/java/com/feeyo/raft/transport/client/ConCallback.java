package com.feeyo.raft.transport.client;

import java.io.IOException;

public interface ConCallback {
	//
	void connectionAcquired(HttpClientConnection conn); // 已获得有效连接的响应处理

	void connectionError(HttpClientConnection conn, Exception e); // 无法获取连接

	void handleResponse(HttpClientConnection conn, byte[] data) throws IOException; // 收到数据包的响应处理

	void connectionClose(HttpClientConnection conn, String reason); // 连接关闭

}
