package com.feeyo.raft;

import com.alibaba.fastjson.annotation.JSONField;

public final class Peer {
	//
	private long id;
	
	@JSONField(serialize=false) 
	private final Endpoint endpoint;
	
	private boolean isLearner;
	//
	// 节点的选举优先级值，如果节点不支持优先级选举，值设置-1
	private int priority = ElectionPriority.Disabled;
	
	//
	public Peer() {
		endpoint = new Endpoint();
	}
	
	public Peer(long id, String ip, int port, boolean isLearner) {
		this.endpoint = new Endpoint(ip, port);
		this.id = id;
		this.isLearner = isLearner;
	}
	
	public Peer(long id, String ip, int port, boolean isLearner, int priority) {
		this.endpoint = new Endpoint(ip, port);
		this.id = id;
		this.isLearner = isLearner;
		this.priority = priority;
	}
	
	public void setId(long id) {
		this.id = id;
	}
	
	public void setIp(String ip) {
		endpoint.ip = ip;
	}
	
	public void setPort(int port) {
		endpoint.port = port;
	}

	public long getId() {
		return id;
	}

	public String getIp() {
		return endpoint.ip;
	}

	public int getPort() {
		return endpoint.port;
	}
	
	public Endpoint getEndpoint() {
		return endpoint;
	}

	public boolean isLearner() {
		return isLearner;
	}
	
	public void setLearner(boolean isLearner) {
		this.isLearner = isLearner;
	}
	
    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    //
	public String toUrl() {
		return new StringBuffer()
				.append("http://")
				.append( endpoint.ip )
				.append(":")
				.append( endpoint.port)
				.append( "/raft/message" )
				.toString();
	}
	
	// 判断该节点是否可以参与选举
	public boolean isPriorityNotElected() {
        return this.priority == ElectionPriority.NotElected;
    }

    // 判断该节点是否禁用了优先选举功能
    public boolean isPriorityDisabled() {
        return this.priority <= ElectionPriority.Disabled;
    }
	
	///
	public static class Endpoint{
		
		public String ip;
		public int port;
		
		public Endpoint() {}
		
		public Endpoint(String ip, int port) {
			super();
			this.ip = ip;
			this.port = port;
		}

        @Override
        public String toString() {
            StringBuffer strBuf = new StringBuffer();
            strBuf.append(ip);
            strBuf.append(":");
            strBuf.append(port);    
            return strBuf.toString();
        }
	}
	
}
