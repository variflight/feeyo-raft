package com.feeyo.raft.caller;

import org.apache.commons.lang3.StringUtils;

import com.feeyo.raft.Const;
import com.feeyo.raft.caller.callback.ProposeCallbackAsyncImpl;
import com.feeyo.raft.caller.callback.ProposeCallbackSyncImpl;
import com.feeyo.raft.caller.callback.ReadIndexCallbackAsyncImpl;
import com.feeyo.raft.caller.callback.ReadIndexCallbackSyncImpl;
import com.feeyo.raft.group.RaftGroup;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.util.ObjectIdUtil;

/**
 * Client for multi-raft
 * 
 * @author zhuam
 *
 */
public class RaftGroupCallerImpl extends RaftCallerAdapter<RaftGroupMessage> {
	
	private RaftGroupServer raftGroupSrv;

	//
	public RaftGroupCallerImpl(RaftGroupServer raftGroupSrv) {
		this.raftGroupSrv = raftGroupSrv;
	}
	
	@Override
	public Reply sync(RaftGroupMessage gMsg) {
		
		long regionId = gMsg.getRegionId();
		Message msg = gMsg.getMessage();
		RaftGroup raftGroup = raftGroupSrv.findGroupByRegionId(regionId);
		if ( raftGroup == null )
			throw new UnsupportedOperationException( String.format( "The region %s was not found in the multi-raft", regionId) );
		
		//
		switch( msg.getMsgType() ) {
		case MsgPropose: {
			String cbKey = super.getCbKey(msg);
			if (StringUtils.isEmpty(cbKey)) {
				cbKey = ObjectIdUtil.getId();
				msg = super.injectKeyToMsg(cbKey, msg);
			}
			//
			ProposeCallbackSyncImpl callback = new ProposeCallbackSyncImpl(cbKey);
			raftGroup.getCallbackRegistry().add( callback );
			raftGroup.local_step(cbKey, msg);
			callback.await();		// raft sync, must wait
			//
			return new Reply(callback.code, callback.msg);
		}
		case MsgReadIndex:{
			if ( msg.getFrom() != Const.None  ) 
				throw new UnsupportedOperationException( "Msg is not local");
			//
			//  from local message
			String key = msg.getEntries(0).getData().toStringUtf8();
			ReadIndexCallbackSyncImpl callback = new ReadIndexCallbackSyncImpl(key);
			raftGroup.getCallbackRegistry().add( callback );
			raftGroup.local_step(key, msg);	// async
			callback.await();				// local, must wait
			//
			return new Reply(callback.code, callback.msg);
		}
		default:
			throw new UnsupportedOperationException( "Not support msgType=" + msg.getMsgType());
		}
	}

	@Override
	public void async(RaftGroupMessage gMsg, ReplyListener replyListener) {
		long regionId = gMsg.getRegionId();
		Message msg = gMsg.getMessage();
		RaftGroup raftGroup = raftGroupSrv.findGroupByRegionId(regionId);
		if ( raftGroup == null )
			throw new UnsupportedOperationException( String.format( "The region %s was not found in the multi-raft", regionId) );
		//
		switch( msg.getMsgType() ) {
		case MsgPropose: {
			String cbKey = super.getCbKey(msg);
			if (StringUtils.isEmpty(cbKey)) {
				cbKey = ObjectIdUtil.getId();
				msg = super.injectKeyToMsg(cbKey, msg);
			}

			raftGroup.getCallbackRegistry().add( new ProposeCallbackAsyncImpl(cbKey, replyListener) );
			raftGroup.local_step(cbKey, msg);
		}
		break;
		case MsgReadIndex:{
			if ( msg.getFrom() != Const.None  ) 
				throw new UnsupportedOperationException( "Msg is not local");
			
			//  from local message
			String key = msg.getEntries(0).getData().toStringUtf8();
			raftGroup.getCallbackRegistry().add( new ReadIndexCallbackAsyncImpl(key, replyListener) );
			raftGroup.local_step(key, msg);
		}
		break;
		default:
			throw new UnsupportedOperationException( "Not support msgType=" + msg.getMsgType());
		}	
	}
}