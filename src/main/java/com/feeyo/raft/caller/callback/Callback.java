package com.feeyo.raft.caller.callback;

public abstract class Callback {
	
	// Action
	public static final class Action {
		//
		public static int Commit = 1;
		public static int Error = 2;
	}
	
	private static byte[] OK = "ok".getBytes();
	
	//
	public String key;
	public long createMs;
	//
	public volatile int code = 0;
	public volatile byte[] msg = null;
	
	public Callback(String key, long createMs) {
		this.key = key;
		this.createMs = createMs;
	}

	// 
	public abstract boolean isTimeout(long nowMs); 

	//
	public void onCompleted() { 
		this.code = 200;
		this.msg = OK;
	}
	
	public void onFailed(byte[] errMsg) {
		this.code = 500;
		this.msg = errMsg;
	}
}
