package com.feeyo.raft;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.Progress.ProgressState;
import com.feeyo.raft.ReadOnly.ReadIndexStatus;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.util.Util;
import com.google.protobuf.ZeroByteStringHelper;

//leader 的状态机
public class StepLeader {
	
	private static Logger LOGGER = LoggerFactory.getLogger( StepLeader.class );
	
    public static void step(Raft r, Message msg) throws RaftException {
    	//
		switch(msg.getMsgType()) {
		case MsgBeat:
			// 广播 HeartBeat 消息
			r.bcastHeartbeat();
			return;
		case MsgCheckQuorum:	
			//检查的原理是发送消息 MsgCheckQuorum 给自己（注意：该消息是由Leader发送给自己，提醒自己进行Leader身份检查）
			if( !r.checkQuorumActive() ) {
				// 如果超过半数的服务器没有活跃
				LOGGER.warn("self={} stepped down to follower since quorum is not active", r.id);
				// 变成follower状态
				r.becomeFollower(r.term, Const.None);
			}
			return;				
		case MsgPropose:
			
			// 不能提交空数据
			if( Util.isEmpty(msg.getEntriesList()) ) {
				LOGGER.error("self={} stepped empty MsgProp", r.id);
				return;
			}
			
			// 检查是否在集群中
			if( r.prs.voters().get( r.id ) == null ) {
				// If we are not currently a member of the range (i.e. this node
				// was removed from the configuration while serving as leader),
				// drop any new proposals.
				throw new Errors.RaftException( Errors.ErrProposalDropped );
			}
			
			// 当前正在转换leader过程中，不能提交
			if ( r.leadTransferee != Const.None) {
				LOGGER.debug("self={}, [term {}] transfer leadership to node={} is in progress; dropping proposal", 
						r.id, r.term, r.leadTransferee);
				throw new Errors.RaftException( Errors.ErrProposalDropped );
			}
			
			// 检查 EntryConfChange
			List<Entry> entries = msg.getEntriesList();
			for (int i = 0; i < entries.size(); i++) {
				//
				Entry entry = entries.get(i);
				if ( entry.getEntryType() == EntryType.EntryConfChange) {
					
					if ( r.pendingConfIndex > r.raftLog.getApplied() ) {
						
						// 如果当前为还没有处理的配置变化请求，则其他配置变化的数据暂时忽略
						LOGGER.info("self={}, propose conf {} ignored since pending unapplied configuration [index {}, applied {}]",
								r.id, entry, r.pendingConfIndex, r.raftLog.getApplied());

						// 将这个数据置空
						entry = Entry.newBuilder()
								.setEntryType(EntryType.EntryNormal)
								.build();
					} else {
						r.pendingConfIndex = r.raftLog.lastIndex() + i + 1;
					}
				}
			}
			
			//
			// 添加数据到 log 中
			if ( !r.appendEntry( msg.getEntriesList() ) ) {
				throw new RaftException( Errors.ErrProposalDropped );
			}
			// 向集群其他节点广播append消息
			r.bcastAppend();
			
			return;
			
		case MsgReadIndex:

			if( r.quorum() > 1 ) {
				
				// raft 5.4 safety 
				// 这个表达式用于判断是否commttied log索引对应的term是否与当前term不相等，
				// 如果不相等说明这是一个新的leader，而在它当选之后还没有提交任何数据
				//
				if( r.raftLog.zeroTermOnErrCompacted(r.raftLog.getCommitted()) != r.term) {
					// Reject read only request when this leader has not committed any log entry at its term.
					return;
				}
				
				// thinking: use an interally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of
                // a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
				switch( r.readOnly.option) {
				case Safe:
					// 把读请求到来时的committed索引保存下来
					r.readOnly.addRequest(r.raftLog.getCommitted(), msg);
					
					// 广播消息出去，其中消息的CTX是该读请求的唯一标识
					// 在应答是Context要原样返回，将使用这个CTX操作readOnly相关数据
					r.bcastHeartbeatWithCtx( ZeroByteStringHelper.getByteArray( msg.getEntries(0).getData() ) ); 
					break;
				
				// read lease 机制
				case LeaseBased:
					
					if (msg.getFrom() == Const.None || msg.getFrom() == r.id ) { // from local member
						// from local message
						// 回调
						String rctx = msg.getEntries(0).getData().toStringUtf8();
						r.listener.onReadIndex( rctx,  r.raftLog.getCommitted() );
						
					} else {
						
						//TODO: ri = r.raftLog.getCommitted();
						//
						long ri = Const.ZERO_IDX;
						if( r.checkQuorum ) {
							ri = r.raftLog.getCommitted();
						}
						
						Message newMsg = Message.newBuilder() //
								.setTo(msg.getFrom()) //
								.setMsgType(MessageType.MsgReadIndexResp) //
								.setIndex( ri ) //
								.addAllEntries(msg.getEntriesList()) //
								.build(); //
						r.send( newMsg );
					}					
				}
				
			} else {
				// rctx
				String rctx =  msg.getEntries(0).getData().toStringUtf8();
				r.listener.onReadIndex( rctx,  r.raftLog.getCommitted() );
			}
			return;
		
		default:
			break;
		}
		
		// All other message types require a progress for m.From (pr)
		// 检查消息发送者当前是否在集群中
		Progress pr = r.prs.get(msg.getFrom());
		if( pr == null ) {
			LOGGER.debug("self={}, no progress available for {}", r.id, msg.getFrom());
			return;
		}
				
		MessageType msgType = msg.getMsgType();
		switch(msgType) {
		case MsgAppendResponse:
			// 对append消息的应答
			
			// 更新该节点当前是活跃的
			pr.setRecentActive(true);
			
			// 如果拒绝了append消息，说明term、index不匹配
			if ( msg.getReject() ) {
				
				LOGGER.debug("self={}, received msgAppend rejection(lastindex: {}) from node={} for msg index {}", r.id,
						msg.getRejectHint(), msg.getFrom(), msg.getIndex());

				// RejectHint 带来的是拒绝该app请求的节点，其最大日志的索引
				// 尝试回退关于该节点的Match、Next索引
				if (pr.maybeDecrTo(msg.getIndex(), msg.getRejectHint())) {
					
					LOGGER.debug("self={}, decreased progress of {} to [{}]", r.id, msg.getFrom(), pr);
					if ( pr.getState() == ProgressState.Replicate ) {
						pr.becomeProbe();
					}
					
					// 再次发送append消息
					r.sendAppend( msg.getFrom() );
				}
				
			} else {
				
				// 通过该append请求
				boolean oldPaused = pr.isPaused();
				
				// 如果该节点的索引发生了更新, 调整该 Progress 的 matched & nextIdx
				if( pr.maybeUpdate( msg.getIndex() ) ) {
					
					switch(pr.getState()) {
					case Probe:
						// 如果当前该节点在探测状态，切换到可以接收副本状态
						pr.becomeReplicate();
						break;
					case Snapshot:
						if ( pr.maybeSnapshotAbort() ) {
							// 如果当前该接在在接受快照状态，而且已经快照数据同步完成了
							LOGGER.debug("self={}, snapshot aborted, resumed sending replication messages to node={} [{}]", r.id, msg.getFrom(), pr);

							// 切换到探测状态
							pr.becomeProbe();
						}
						break;
					case Replicate:
						// 如果当前该节点在接收副本状态，因为通过了该请求，所以滑动窗口可以释放在这之前的索引了
						pr.getIns().freeTo( msg.getIndex() );
						break;
					}
					
					// 如果可以commit日志，那么广播append消息
					if ( r.maybeCommit() ) {
						r.bcastAppend();
						
					} else if (oldPaused) {
						// If we were paused before, this node may be missing the latest commit index, so send it.
						r.sendAppend( msg.getFrom() );
					}
					
					// We've updated flow control information above, which may
					// allow us to send multiple (size-limited) in-flight messages
					// at once (such as when transitioning from probe to replicate, or when freeTo() covers multiple messages). 
					// If we have more entries to send, send as many messages as we
					// can (without sending empty messages for the commit index)
					while( r.maybeSendAppend(msg.getFrom(), false) ) {
						//
					}
					
					// Transfer leadership is in progress.
					if (msg.getFrom() == r.leadTransferee && pr.getMatched() == r.raftLog.lastIndex()) {
						LOGGER.info("self={}, sent MsgTimeoutNow to node={} after received MsgAppResp", r.id, msg.getFrom());
						// 要迁移过去的新leader，其日志已经追上了旧的leader
						r.sendTimeoutNow( msg.getFrom() );
					}
				}
			}
			break;
		
		case MsgHeartbeatResponse:
			
			// 该节点当前处于活跃状态
			pr.setRecentActive(true);
			
			// 这里调用resume是因为当前可能处于probe状态，而这个状态在两个HeartBeat消息的间隔期只能收一条同步日志消息，
			// 因此在收到HeartBeat消息时就停止pause标记
			pr.resume();
			
			//Fix bug (ToCommit Exception): mem & restart follower node 导致node的lastIndex < leader.prs.get(id).matched
			if(msg.getReject()) {
				pr.setMatched( msg.getRejectHint() );
				pr.becomeProbe();
			}
			
			// free one slot for the full inflights window to allow progress.
			if (pr.getState() == ProgressState.Replicate && pr.getIns().full()) {
				pr.getIns().freeFirstOne();
			}

			// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
			if (pr.getMatched() < r.raftLog.lastIndex()) {
				// @see https://github.com/etcd-io/etcd/blob/master/raft/raft.go
				// case pb.MsgHeartbeatResp:
				r.sendAppend( msg.getFrom() );
			}
			
			// 只有ReadOnly safe方案，才会继续往下走
			if (r.readOnly.option != ReadOnlyOption.Safe || msg.getContext().isEmpty()) {
				return;
			}
			
			// 收到应答调用recvAck函数返回当前针对该消息已经应答的节点数量
		    long ackCount = r.readOnly.recvAck( msg );
		    if (ackCount < r.quorum()) {
		    	// 小于集群半数以上就返回不往下走了
		    	return;
		    }
         				
		    // 调用advance函数尝试丢弃已经被确认的read index状态
			List<ReadIndexStatus> rss = r.readOnly.advance(msg);
		    if( Util.isNotEmpty( rss ) ){
		    	
		    	// 遍历准备被丢弃的 ReadIndex 状态
				for (ReadIndexStatus rs : rss) {
					
					Message req = rs.req;
					if (req.getFrom() == Const.None || req.getFrom() == r.id) {
						// 如果来自本地
						String rctx = req.getEntries(0).getData().toStringUtf8();
						r.listener.onReadIndex( rctx,  rs.index);
						
					} else {
						// 否则就是来自外部，需要应答
						Message newMsg = Message.newBuilder() //
								.setTo(req.getFrom()) //
								.setMsgType(MessageType.MsgReadIndexResp) //
								.setIndex(rs.index) //
								.addAllEntries(req.getEntriesList()) //
								.build(); //

						r.send(newMsg);
					}
				}
			}
			break;
		
		case MsgSnapStatus:
			// 当前该节点状态已经不是在接受快照的状态了，直接返回
			if (pr.getState() != ProgressState.Snapshot) {
				return;
			}

			if (!msg.getReject() ) {
				// 接收快照成功
				pr.becomeProbe();
				LOGGER.debug("self={}, snapshot succeeded, resumed sending replication messages to node={} [{}]", r.id, msg.getFrom(), pr);
				
			} else {
				// 接收快照失败
				pr.snapshotFailure();
				pr.becomeProbe();
				LOGGER.debug("self={}, snapshot failed, resumed sending replication messages to {} [{}]", r.id, msg.getFrom(), pr);	
			}
			
			// If snapshot finish, wait for the msgAppResp from the remote node before sending out the next msgApp.
			// If snapshot failure, wait for a heartbeat interval before next try
			// 先暂停等待下一次被唤醒
			pr.pause();
			break;
		
		case MsgUnreachable:				
			// During optimistic replication, if the remote becomes unreachable, 
			// there is huge probability that a MsgApp is lost.
			if( pr.getState() == ProgressState.Replicate) {
				pr.becomeProbe();
			}
			LOGGER.debug("self={}, failed to send message to node={} because it is unreachable [{}]", r.id, msg.getFrom(), pr);
			break;
		
		case MsgTransferLeader:
			if( pr.isLearner() ) {
				LOGGER.debug("self={}, is learner. Ignored transferring leadership", r.id);
				return;
			}
			
			long leadTransferee = msg.getFrom();
			long lastLeadTransferee = r.leadTransferee;
			if ( lastLeadTransferee != Const.None ) {
				// 判断是否已经有相同节点的leader转让流程在进行中
				if (lastLeadTransferee == leadTransferee) {
					LOGGER.info("self={}, [term {}] transfer leadership to node={} is in progress, ignores request to same node={}",
							r.id, r.term, leadTransferee, leadTransferee);
					// 如果是，直接返回
					return;
				}
				
				// 否则中断之前的转让流程
				r.abortLeaderTransfer();
				LOGGER.info("self={}, [term {}] abort previous transferring leadership to node={}", r.id, r.term, lastLeadTransferee);
			}
			
			// 判断是否转让过来的leader是否本节点，如果是也直接返回，因为本节点已经是leader了
			if ( leadTransferee == r.id ) {
				LOGGER.debug("self={}, is already leader. Ignored transferring leadership to self", r.id);
				return;
			}
			
			// Transfer leadership to third party.
	        LOGGER.info("self={}, [term {}] starts to transfer leadership to node={}", r.id, r.term, leadTransferee);
	        
	        // Transfer leadership should be finished in one electionTimeout
	        // so reset r.electionElapsed.
	        r.electionElapsed = 0;
	        r.leadTransferee = leadTransferee;
			if (pr.getMatched() == r.raftLog.lastIndex()) {
				// 如果日志已经匹配了，那么就发送 TimeoutNow协议过去
				r.sendTimeoutNow(leadTransferee);
				//
				LOGGER.info("self={}, sends MsgTimeoutNow to node={} immediately as node={} already has up-to-date log",
								r.id, leadTransferee, leadTransferee);
			} else {
				// 否则继续追加日志
				r.sendAppend(leadTransferee);
			}
			break;
		default:
			break;
		}
    }
}
