package com.feeyo.raft.caller.callback;

import com.feeyo.net.nio.util.TimeUtil;

public class ProposeCallbackSyncImpl extends ProposeCallback {
	
	final Object monitor = new Object();
	boolean isNotified = false;

	public ProposeCallbackSyncImpl(String key) {
		super(key, TimeUtil.currentTimeMillis());
	}
	
	@Override
	public void onCompleted() {	
		super.onCompleted();
		_notify();
	}
	
	@Override
	public void onFailed(byte[] errMsg) {
		super.onFailed(errMsg);
		_notify();
	}
	
	public void await() {
		_wait();
	}
	
	private void _notify() {
		synchronized( monitor ) {
			isNotified = true;
			monitor.notify();
		}
	}
	
	private void _wait() {
		synchronized (monitor) {
			if (!isNotified) {
				try {
					monitor.wait();
				} catch (InterruptedException e) { 
					/* ignored */
				}
			}
		}
	}
}
