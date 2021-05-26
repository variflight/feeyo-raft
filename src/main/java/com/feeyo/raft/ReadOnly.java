package com.feeyo.raft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import com.feeyo.raft.proto.Raftpb.Message;
import com.google.protobuf.ByteString;


// @see https://github.com/coreos/etcd/blob/master/raft/read_only.go
//
// @see https://aphyr.com/posts/313-strong-consistency-models
//
// 用于管理全局 ReadIndex 数据
//
public class ReadOnly {
	//
	// Linearizable Read 选项， Safe or LeaseBased
	public final ReadOnlyOption option;
	//
	private Map<String, ReadIndexStatus> pendingReadIndex = new ConcurrentHashMap<String, ReadIndexStatus>(); 	// 待处理的 ReadIndex 请求，使用entry的数据为key
	private LinkedBlockingDeque<String> readIndexQueue = new LinkedBlockingDeque<String>( 5000 );	// 所有的 ReadIndex
	
	public ReadOnly(ReadOnlyOption option) {
		this.option = option;
	}
	
	/// add_request adds a read only request into readonly struct.
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    /// `msg` is the original read only request message from the local or remote node.
	public void addRequest(long index, Message msg) {

		// 判断是否重复添加
		String ctx = msg.getEntries(0).getData().toStringUtf8();
		if ( !pendingReadIndex.containsKey(ctx) ) {
			
//			if (LoggerUtil.isDebugEnabled())
//				LoggerUtil.debug("#addRequest, ctx={} index={} ",ctx, index);
			
			ReadIndexStatus status = new ReadIndexStatus(msg, index);
			pendingReadIndex.put(ctx, status);
			readIndexQueue.add(ctx);
		}
	}

	/// rev_ack notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
	
	// 收到某个节点对一个 heartBeat 消息的应答，这个函数中尝试查找该消息是否在readOnly数据中
	public long recvAck(Message msg) {

		// 根据context内容到map中进行查找， 找不到就返回，说明这个heartBeat消息没有带上readIndex相关的数据
		String ctx = msg.getContext().toStringUtf8();
		ReadIndexStatus readIndexStatus = pendingReadIndex.get(ctx);
		if ( readIndexStatus == null ) {
			return 0;
		}
		
		// 将该消息的Ask map加上该应答节点
		readIndexStatus.acks.add( msg.getFrom() );
		
		// 返回当前Ack的节点数量，+1是因为包含了leader
		long count = readIndexStatus.acks.size() + 1;
		
//		if (LoggerUtil.isDebugEnabled())
//			LoggerUtil.debug("#ReadOnly recvAsk, ctx={}, askCount={}", ctx, count );
		
		return count;
	}
	
	
	// 在确保某 heartBeat消息被集群中半数以上节点应答了，此时尝试在readIndex队列中查找，看一下队列中的readIndex数据有哪些可以丢弃了（也就是已经被应答过了）
	// 最后返回被丢弃的数据队列
	public List<ReadIndexStatus> advance(Message msg) {
		//
		List<ReadIndexStatus> rss = new ArrayList<ReadIndexStatus>();
		int idx = 0;
		boolean found = false;
		String msgCtx = msg.getContext().toStringUtf8();
		for(String ctx : readIndexQueue ) {
			idx++;
			//  由于并发问题，此处可能会出现在map中找不到该数据的情况，   XXXXXX：不可能出现在map中找不到该数据的情况
			ReadIndexStatus rs = pendingReadIndex.get(ctx);
			if( rs == null) {
				//LoggerUtil.error("cannot find correspond read state from pending map, ctx={}", ctx);
				return null;
			}

			if( ctx.equals(msgCtx) ) {
				found = true;
				break;
			}
		}
		
		if ( found ) {
			// 将该readIndex之前的所有readIndex请求都认为是已经成功进行确认的了，所有成功确认的readIndex请求
			for(int i = 0; i < idx; i++) {
				String ctx = readIndexQueue.pollFirst();
				if ( ctx != null ) {
					ReadIndexStatus rs = pendingReadIndex.remove( ctx );
					if ( rs != null ) {
						rss.add( rs );
					}
				}
			}
		}
		return rss;
	}
	
	/// last_pending_request_ctx returns the context of the last pending read only
    /// request in ReadOnly struct.
	
	// 从readOnly队列中返回最后一个数据
	public byte[] lastPendingRequestCtx() {
		if ( !readIndexQueue.isEmpty() ) {
			String ctx = readIndexQueue.peekLast();
			if ( ctx != null )
				return ctx.getBytes();
		}
		return null;
	}
	
	public int pendingReadCount() {
		return readIndexQueue.size();
	}
	
	//
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	
	// ReadState provides state for read only query.
	// It's caller's responsibility to call ReadIndex first before getting
	// this state from ready, it's also caller's duty to differentiate if this
	// state is what it requests through RequestCtx, eg. given a unique id as
	// RequestCtx
	public static class ReadState {
	    public long index;	// 保存接收该读请求时的committed index
	    public ByteString requestCtx; // 保存读请求ID，全局唯一的定义一次读请求
	    
		public ReadState(long index, ByteString requestCtx) {
			this.index = index;
			this.requestCtx = requestCtx;
		}  
	}
	
	public static class ReadIndexStatus {
		public Message req; // 保存原始的readIndex请求消息
		public long index;	// 保存收到该readIndex请求时的leader commit索引
		public HashSet<Long> acks =  new HashSet<Long>();	// 保存有什么节点进行了应答，从这里可以计算出来是否有超过半数应答了
		
		public ReadIndexStatus(Message req, long index) {
			this.req = req;
			this.index = index;
		}
	}
}