package com.feeyo.raft.caller.callback;

import java.util.concurrent.atomic.AtomicBoolean;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.caller.RaftCaller;

public class ProposeCallbackAsyncImpl extends ProposeCallback {
	
	private RaftCaller.ReplyListener replyListener;
	
	// Ensuring thread safe
	private AtomicBoolean running = new AtomicBoolean(false);
	
	public ProposeCallbackAsyncImpl(String key, RaftCaller.ReplyListener listener) {
		super(key, TimeUtil.currentTimeMillis());
		this.replyListener = listener;
	}
	
	@Override
	public void onCompleted() {
		
		if ( !running.compareAndSet(false, true) )
			return;
		
		super.onCompleted();
		replyListener.onCompleted();
	}
	
	@Override
	public void onFailed(byte[] errMsg) {
		
		if ( !running.compareAndSet(false, true) )
			return;
		
		super.onFailed( errMsg );
		replyListener.onFailed(code, msg);
	}
}
