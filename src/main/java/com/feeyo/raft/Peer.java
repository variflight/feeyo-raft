package com.feeyo.raft;

import com.alibaba.fastjson.annotation.JSONField;

public final class Peer {
	
	private long id;
	
	@JSONField(serialize=false) 
	private final Endpoint endpoint;
	
	private boolean isLearner;
	
	// Used to follower replication
	// member group id
	// 
	private long gid;	
	
	public Peer() {
		endpoint = new Endpoint();
	}
	
	public Peer(long id, String ip, int port, boolean isLearner) {
		this.endpoint = new Endpoint(ip, port);
		this.id = id;
		this.isLearner = isLearner;
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
	
	public long getGid() {
		return gid;
	}

	public void setGid(long gid) {
		this.gid = gid;
	}

	public String toUrl() {
		
		return new StringBuffer()
				.append("http://")
				.append( endpoint.ip )
				.append(":")
				.append( endpoint.port)
				.append( "/raft/message" )
				.toString();
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
            return new StringBuffer()
                    .append(ip)
                    .append(":")
                    .append(port)
                    .toString();
        }
	}
	
}
