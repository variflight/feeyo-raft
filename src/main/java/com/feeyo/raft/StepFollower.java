package com.feeyo.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;

//follower 的状态机函数
public class StepFollower {
	
	private static Logger LOGGER = LoggerFactory.getLogger( StepFollower.class );
	
	public static void step(Raft r, Message msg) throws RaftException {
		
		MessageType msgType = msg.getMsgType();
		switch(msgType) {
		case MsgPropose:	
			// 本节点提交的值
			if( r.leaderId == Const.None ) {
				// 没有leader则提交失败，忽略
				LOGGER.info("self={}, no leader at term {}; dropping proposal", r.id, r.term);
				return;
			} 
			//
			if (r.disableProposalForwarding) {
				LOGGER.info("self={} not forwarding to leader {} at term {}; dropping proposal", r.id, r.leaderId, r.term);
                return;
            }
			
			// 向 leader 进行 redirect
			msg = msg.toBuilder() //
					.setTo(r.leaderId) //
					.build(); //
			
			r.send(msg);
			break;
			
		case MsgAppend:
			// 收到leader的 append 消息，重置选举tick计时器，因为这样证明leader还存活
			r.electionElapsed = 0;
			updateLeaderId(r, msg.getFrom());
			r.handleAppendEntries(msg);
			break;
			
		case MsgHeartbeat:
			// 收到leader的 heartbeat 消息，重置选举tick计时器，因为这样证明leader还存活
			r.electionElapsed = 0;
			updateLeaderId(r, msg.getFrom());
			r.handleHeartbeat(msg);
			break;
			
		case MsgSnapshot:
			// 快照消息
			r.electionElapsed = 0;
			updateLeaderId(r, msg.getFrom());
			r.handleSnapshot(msg);
			break;
			
		case MsgTransferLeader:
			if( r.leaderId == Const.None) {
				LOGGER.info("self={}, no leader at term {}; dropping leader transfer msg", r.id, r.term);
				return;
			}
			
			// 向leader转发leader转让消息
			msg = msg.toBuilder()
					.setTo( r.leaderId )
					.build();
			r.send(msg);
			break;
			
		case MsgTimeoutNow:
			
			// 如果本节点可以提升为leader，那么就发起新一轮的竞选
			if( r.promotable() ) {
				// Leadership transfers never use pre-vote even if self.pre_vote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
				LOGGER.info("self={}, [term {}] received MsgTimeoutNow from node={} and starts an election to  get leadership.", r.id, r.term, msg.getFrom());
				
				// timeout消息用在leader转让中，所以不需要preVote即使开了这个选项
				r.campaign(CampaignType.CAMPAIGN_TRANSFER);
				
			} else {
				LOGGER.info("self={}, received MsgTimeoutNow from node={} but is not promotable", r.id, msg.getFrom());
			}
			break;
			
		case MsgReadIndex:
			if( r.leaderId == Const.None ) {
				LOGGER.warn("self={}, no leader at term {}; dropping index reading msg", r.id, r.term);
				//
				String rctx = msg.getEntries(0).getData().toStringUtf8();
				r.listener.onReadIndex(rctx,  -1 );
				return;
			}
			
			// 向leader转发此类型消息
			msg = msg.toBuilder()
					.setTo( r.leaderId )
					.build();
			r.send(msg);
			break;
			
		case MsgReadIndexResp:	
			
			if (msg.getEntriesList().size() != 1) {
				LOGGER.error("self={}, invalid format of MsgReadIndexResp from {}, entries count: {}", r.id,
						msg.getFrom(), msg.getEntriesList().size());
				return;
			}
			
			// MsgReadIndexResp, 通知
			String rctx = msg.getEntries(0).getData().toStringUtf8();
			r.listener.onReadIndex( rctx,  msg.getIndex() );
			break;
			
		default: 
			break;
		}
	}
	
	private static void updateLeaderId(Raft r, long leaderId) {
		// 防止初始化到选举成功之后 一直是follower状态的节点，未能及时把leaderId传给状态机
		if (r.leaderId != leaderId) {
			r.leaderId = leaderId;
			r.listener.onStateChange( r.id, StateType.Follower, r.leaderId );
		}
	}
}
