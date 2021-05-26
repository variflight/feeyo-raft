package com.feeyo.raft.transport.client.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.feeyo.net.nio.NIOConnector;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.transport.client.HttpClientConnectionFactory;
import com.feeyo.raft.transport.client.PhysicalNode;

/**
 * 客户端连接池
 * 
 * @author zhuam
 *
 */
public class ClientNodePool {
	
	private static final long TIMEOUT =  2 * 60 * 1000L;
	
	private AtomicBoolean checkFlag = new AtomicBoolean(false);
	private HttpClientConnectionFactory connectionFactory = null;
	
	private int minCon = 3;
	private int maxCon = 50;
	private Map<Long, ClientNode> nodes = new HashMap<Long, ClientNode>();
	
	public ClientNodePool(NIOConnector nioConnector) {		
		this.connectionFactory = new HttpClientConnectionFactory( nioConnector );
	}
	
	public boolean start(PeerSet peerSet) {	
		for( Peer peer: peerSet.values() ) {
			long id = peer.getId();
			String ip = peer.getIp();
			int port = peer.getPort();
			nodes.put(id, new ClientNode(connectionFactory, id, ip, port, minCon, maxCon));
		}
		return true;
	}

	public void stop() {
		for (ClientNode node : nodes.values())
			node.stop();
		
		nodes.clear();
	}
	
	/**
	 * 连接池心跳 检测
	 * 1、IDLE 连接的有效性检测，无效 close
	 * 2、连接池过大、过小的动态调整
	 */
	public void check() {
		if (!checkFlag.compareAndSet(false, true))
			return;
		try {
			for (ClientNode node: nodes.values()) {		
				node.availableCheck();	// 有效性检测
				long now = TimeUtil.currentTimeMillis();
				long heartbeatTime = now - TIMEOUT;
				long closeTime = now - TIMEOUT * 2;
				node.heartbeatCheck(heartbeatTime, closeTime); 	// 心跳检测
			}
			
		} catch(Throwable e) {
			// ignore
		} finally {
			checkFlag.set(false);
		}
	}

	public PhysicalNode getPhysicalNode(long id) {
		ClientNode node = nodes.get(id);
		if ( node != null )
			return node.getPhysicalNode();
		//
		return null;
	}
}