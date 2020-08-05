package com.feeyo.raft.caller.callback;
///
// Propose Callback
//
public abstract class ProposeCallback extends Callback {
	
	private static final int TIMEOUT = 5 * 60 * 1000;		
	//
	public ProposeCallback(String key,long lastTime) {
		super(key, lastTime);
	}
	
	@Override
	public boolean isTimeout(long nowMs) {
		if ( nowMs - createMs > TIMEOUT )
			return true;
		return false;
	}
}
