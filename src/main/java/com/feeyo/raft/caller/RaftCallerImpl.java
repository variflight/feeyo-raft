package com.feeyo.raft.caller;

import com.feeyo.raft.RaftServer;

import org.apache.commons.lang3.StringUtils;

import com.feeyo.raft.Const;
import com.feeyo.raft.caller.callback.ProposeCallbackAsyncImpl;
import com.feeyo.raft.caller.callback.ReadIndexCallbackAsyncImpl;
import com.feeyo.raft.caller.callback.ProposeCallbackSyncImpl;
import com.feeyo.raft.caller.callback.ReadIndexCallbackSyncImpl;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.util.ObjectIdUtil;

//
// 进程内与 raftServer 进行通讯，可节省消耗
public class RaftCallerImpl extends RaftCallerAdapter<Message> {
	
	private RaftServer raftSrv;
	
	public RaftCallerImpl(RaftServer raftSrv) {
		this.raftSrv = raftSrv;
	}

	@Override
	public Reply sync(Message msg) {
		//
		switch( msg.getMsgType() ) {
		case MsgPropose: {
			//
			String cbKey = super.getCbKey(msg);
			if (StringUtils.isEmpty(cbKey)) {
				cbKey = ObjectIdUtil.getId();
				msg = super.injectKeyToMsg(cbKey, msg);
			}
			//
			ProposeCallbackSyncImpl callback = new ProposeCallbackSyncImpl(cbKey);
			raftSrv.getCallbackRegistry().add( callback );
			raftSrv.local_step(cbKey, msg);
			callback.await();		// raft sync, must wait
			//
			return new Reply(callback.code, callback.msg);
		}
		case MsgReadIndex:{
			//
			if ( msg.getFrom() != Const.None  ) 
				throw new UnsupportedOperationException( "Msg is not local");
			//
			//  from local message
			String key = msg.getEntries(0).getData().toStringUtf8();
			ReadIndexCallbackSyncImpl callback = new ReadIndexCallbackSyncImpl(key);
			raftSrv.getCallbackRegistry().add( callback );
			raftSrv.local_step(key, msg);	// async
			callback.await();				// local, must wait
			//
			return new Reply(callback.code, callback.msg);
		}
		default:
			throw new UnsupportedOperationException( "Not support msgType=" + msg.getMsgType());
		}
		
	}
	
	@Override
	public void async(Message msg, ReplyListener replyListener) {
		//
		switch (msg.getMsgType()) {
		case MsgPropose: {
			String cbKey = super.getCbKey(msg);
			if (StringUtils.isEmpty(cbKey)) {
				cbKey = ObjectIdUtil.getId();
				msg = super.injectKeyToMsg(cbKey, msg);
			}
			//
			raftSrv.getCallbackRegistry().add(new ProposeCallbackAsyncImpl(cbKey, replyListener));
			raftSrv.local_step(cbKey, msg);
		}
		break;
		case MsgReadIndex:{
			//
			if ( msg.getFrom() != Const.None  ) 
				throw new UnsupportedOperationException( "Msg is not local");
			
			//  from local message
			String key = msg.getEntries(0).getData().toStringUtf8();
			raftSrv.getCallbackRegistry().add( new ReadIndexCallbackAsyncImpl(key, replyListener) );
			raftSrv.local_step(key, msg);
		}
		break;
		default:
			throw new UnsupportedOperationException( "Not support msgType=" + msg.getMsgType());
		}
	}
}