package com.feeyo.raft.caller.callback;

import java.util.concurrent.atomic.AtomicBoolean;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.caller.RaftCaller;

public class ReadIndexCallbackAsyncImpl extends ReadIndexCallback {
	
	private RaftCaller.ReplyListener replyListener;
	
	// Ensuring thread safe
	private AtomicBoolean running = new AtomicBoolean(false);

	public ReadIndexCallbackAsyncImpl(String key, RaftCaller.ReplyListener replyListener) {
		super(key, TimeUtil.currentTimeMillis());

		//
		this.replyListener = replyListener;
	}

	@Override
	public void onCompleted() {
		//
		if (!running.compareAndSet(false, true))
			return;
		super.onCompleted();
		replyListener.onCompleted();
	}

	@Override
	public void onFailed(byte[] errMsg) {
		//
		if (!running.compareAndSet(false, true))
			return;
		super.onFailed(errMsg);
		replyListener.onFailed(code, msg);
	}
}
