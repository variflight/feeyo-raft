package com.feeyo.raft.caller.callback;

// ReadIndex Callback ( for local MsgReadIndex only )
///
public abstract class ReadIndexCallback extends Callback {		
	
	private static final int TIMEOUT = 3 * 1000;			// 3 Seconds
	
	// from the leader  
	public long index = 0;
	public long recvMs = 0;

	public ReadIndexCallback(String key, long createMs) {
		super(key, createMs);
	}

	@Override
	public boolean isTimeout(long nowMs) {
		if ( nowMs - createMs > TIMEOUT )
			return true;
		return false;
	}
}