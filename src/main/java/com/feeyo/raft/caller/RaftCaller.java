package com.feeyo.raft.caller;


// A client send requests to a raft server
//
public interface RaftCaller<M> {

	// MsgPropose & MsgReadIndex
	//
	public Reply sync(M msg);
	public void async(M msg, ReplyListener listener);

	// -------------------------------
	//
	public static class Reply {
		
		public int code;
		public byte[] msg;

		public Reply(int code, byte[] msg) {
			this.code = code;
			this.msg = msg;
		}
	}
	
	public static interface ReplyListener {
		//
		public void onCompleted();
		public void onFailed(int errCode, byte[] errMsg);
	}
	
}