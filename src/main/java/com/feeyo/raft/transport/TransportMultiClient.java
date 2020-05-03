package com.feeyo.raft.transport;

import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.raft.group.proto.Raftgrouppb;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.transport.client.AsyncHttpResponseHandler;
import com.feeyo.raft.transport.client.util.RaftGroupMessageUtil;

/**
 * Used for multiple raft groups
 * 
 * @author zhuam
 *
 */
public class TransportMultiClient extends AbstractTransportClient<Raftgrouppb.RaftGroupMessage> {
	//
	@Override
	public boolean syncPost(RaftGroupMessage message) {
		try {
			long to = message.getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer( message );
			return syncSend(to, buffer);
		} catch(Throwable e) {
			return false;
		}
	}
	
	@Override
	public boolean syncBatchPost(List<RaftGroupMessage> messages) {
		try {
			long to = messages.get(0).getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer( messages );
			return syncSend(to, buffer);
		} catch(Throwable e) {
			return false;
		}
	}
	
	//
	@Override
	public void asyncPost(RaftGroupMessage message) {
		asyncPost(message, null);
	}
	
	@Override
	public void asyncPost(RaftGroupMessage message, AsyncHttpResponseHandler handler) {
		try {
			long to = message.getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer( message );
			asyncSend(to, buffer, handler);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	@Override
	public void asyncBatchPost(List<RaftGroupMessage> messages) {
		try {
			long to = messages.get(0).getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer(messages);
			asyncSend(to, buffer, null);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	//
	@Override
	public void pipeliningPost(RaftGroupMessage message) {
		try {
			long to = message.getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer( message );
			pipeliningSend(to, buffer);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	@Override
	public void pipeliningBatchPost(List<RaftGroupMessage> messages) {
		try {
			long to = messages.get(0).getToPeer().getId();
			ByteBuffer buffer = RaftGroupMessageUtil.toByteBuffer(messages);
			pipeliningSend(to, buffer);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	//
	// 待支持均衡性连接算法
	protected boolean pipeliningSend(long id, ByteBuffer buffer) {
		return super.pipeliningSend(id, buffer);
	}
}