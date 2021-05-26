package com.feeyo.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;

//
// stepCandidate is shared by StateCandidate and StatePreCandidate; 
// the difference is whether they respond to MsgVoteResp or MsgPreVoteResp.
public class StepCandidate {
	
	private static Logger LOGGER = LoggerFactory.getLogger( StepCandidate.class );
	
	// Only handle vote responses corresponding to our candidacy (while in
    // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
    // our pre-candidate state).
    public static synchronized void step(Raft r, Message msg) throws RaftException {
        MessageType msgType = msg.getMsgType();
		switch (msgType) {
		case MsgPropose:
			// 当前没有leader，所以忽略掉提交的消息
			LOGGER.info("self={}, no leader at term {}; dropping proposal", r.id, r.term);
			LOGGER.error("{} raft: proposal dropped", r.id);
			return;
		
		case MsgAppend:
			// 收到append消息，说明集群中已经有leader，转换为follower
			r.becomeFollower(msg.getTerm(), msg.getFrom()); 	// always msg.Term == this.term
			r.handleAppendEntries(msg);
			break;
		case MsgHeartbeat:
			LOGGER.info("self={}, received MsgHeartbeat {}", r.id, ProtobufUtils.protoToJson(msg));
			//
			// 收到 heartbeat 消息，说明集群已经有leader，转换为follower
			r.becomeFollower(msg.getTerm(), msg.getFrom());	// always msg.Term == this.term
			r.handleHeartbeat(msg);
			break;
		case MsgSnapshot:
			// 收到快照消息，说明集群已经有leader，转换为follower
			r.becomeFollower(msg.getTerm(), msg.getFrom()); 	// always msg.Term == this.term
			r.handleSnapshot(msg);
			break;
		case MsgRequestPreVoteResponse:
		case MsgRequestVoteResponse:
			//
			MessageType myVoteRespType;
	        if (r.state == StateType.PreCandidate) {
	            myVoteRespType = MessageType.MsgRequestPreVoteResponse;
	        } else {
	            myVoteRespType = MessageType.MsgRequestVoteResponse;
	        }
	        
			//
			if (msgType != myVoteRespType)
				return;
			//
			// 计算当前集群中有多少节点给自己投了票
			int gr = r.poll(msg.getFrom(), msgType, !msg.getReject());			
			LOGGER.info("self={}, state={} [quorum:{}] has received {} {} votes and {} vote rejections", 
					r.id, r.state, r.quorum(), gr, msgType, r.votes.size() - gr);
		
			// if become leader
			int q = r.quorum();
			if ( q == gr) {  
				// 如果进行投票的节点数量正好是半数以上节点数量
				if (r.state == StateType.PreCandidate) {
					r.campaign( CampaignType.CAMPAIGN_ELECTION );
				} else {
					// 变成leader
					r.becomeLeader();
					r.bcastAppend();
				}
			} else if ( q == r.votes.size() - gr) {
				// 如果是半数以上节点拒绝了投票
				// 变成follower
				r.becomeFollower(r.term, Const.None);
			}
			break;
		case MsgTimeoutNow:
			// candidate 收到 timeout 指令忽略不处理
			LOGGER.debug("self={}, [term {} state {}] ignored MsgTimeoutNow from {}", 
					r.id, r.term, r.state, msg.getFrom());
			break;
		default:
			break;
		}
	}
}