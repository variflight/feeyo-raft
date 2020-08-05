package com.feeyo.raft.transport;

import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.transport.client.AsyncHttpResponseHandler;
import com.feeyo.raft.transport.client.util.RaftMessageUtil;

/**
 * Raft 消息传输
 *  
 * @author zhuam
 */
public class TransportClient extends AbstractTransportClient<Raftpb.Message> {
	
	@Override
	public boolean syncPost(Message message) {
		try {
			long id = message.getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer( message );
			return syncSend(id, buffer);
		} catch(Throwable e) {
			return false;
		}
	}
	
	@Override
	public boolean syncBatchPost(List<Message> messages) {
		try {
			long id = messages.get(0).getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer( messages );
			return syncSend(id, buffer);
		} catch(Throwable e) {
			return false;
		}
	}
	
	//
	@Override
	public void asyncPost(Message message) {
		asyncPost(message, null);
	}
	
	@Override
	public void asyncPost(Message message, AsyncHttpResponseHandler handler) {
		try {
			long to = message.getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer( message );
			asyncSend(to, buffer, handler);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	@Override
	public void asyncBatchPost(List<Message> messages) {
		try {
			long to = messages.get(0).getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer(messages);
			asyncSend(to, buffer, null);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	//
	@Override
	public void pipeliningPost(Message message) {
		try {
			long to = message.getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer( message );
			pipeliningSend(to, buffer);
		} catch(Throwable e) {
			// ignored
		}
	}
	
	@Override
	public void pipeliningBatchPost(List<Message> messages) {
		try {
			long to = messages.get(0).getTo();
			ByteBuffer buffer = RaftMessageUtil.toByteBuffer(messages);
			pipeliningSend(to, buffer);
		} catch(Throwable e) {
			// ignored
		}
	}
}