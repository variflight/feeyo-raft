package com.feeyo.raft;

public class Errors {
	
	// ErrProposalDropped 当 proposal 被忽略时返回，这样就可以快速的通知到 proposer.
	public static final String ErrProposalDropped  = "raft proposal dropped";
	
	// ErrCompacted 某个索引已经被快照保存时返回该错误
	public static final String ErrCompacted = "requested index is unavailable due to compaction";

	// ErrSnapOutOfDate， 当调用 createSnapshot函数时传入的索引比当前的快照索引更小时返回
	public static final String ErrSnapOutOfDate = "requested index is older than the existing snapshot";
	
	// ErrNoSnapshot， 没有有效的快照
	public static final String ErrNoSnapshot = "no available snapshot";
	
	// ErrEmptySnapshot 快照空
	public static final String ErrEmptySnapshot = "empty snapshot";
	
	// ErrUnavailable， 请求的log不可用时返回
	public static final String ErrUnavailable = "requested entry at index is unavailable";
	
	// ErrSnapshotTemporarilyUnavailable， 快照暂时不可用时返回
	public static final String ErrSnapshotTemporarilyUnavailable = "snapshot is temporarily unavailable";
	
	// ErrStepLocalMsg， 当尝试执行本地 raft message 时返回 
	public static final String ErrStepLocalMsg = "raft: cannot step raft local message";
	
	// ErrStepLocalResponseMsg, 当尝试执行本地的  raft response message 时返回
	public static final String ErrStepLocalResponseMsg = "raft: cannot step raft local response message";

	// ErrStepPeerNotFound， 在尝试响应消息时,但没有找到 peer 的时候返回
	public static final String ErrStepPeerNotFound = "raft: cannot step as peer not found";
	
	//
	public static class RaftException extends Exception {

		private static final long serialVersionUID = -1707432311436347340L;

		public RaftException() {
	        super();
	    }

	    public RaftException(String message) {
	        super(message);
	    }

	    public RaftException(String message, Throwable cause) {
	        super(message, cause);
	    }

	    public RaftException(Throwable cause) {
	        super(cause);
	    }

	    protected RaftException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
	        super(message, cause, enableSuppression, writableStackTrace);
	    }
	}
	
	
	//
	public static class RaftConfigException extends RaftException {

		private static final long serialVersionUID = -90760924368934460L;

		public RaftConfigException() {
			super();
		}

		public RaftConfigException(String message) {
			super(message);
		}
	}
	
}
