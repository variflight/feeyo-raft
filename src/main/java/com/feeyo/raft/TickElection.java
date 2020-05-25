package com.feeyo.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;

//follower 以及 candidate的 tick函数，在 r.electionTimeout 之后被调用
public class TickElection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( TickElection.class );

    public static void tick(Raft r) throws RaftException {
    	
    	r.electionElapsed++;
    	
		if (r.promotable() && r.pastElectionTimeout()) {
			
			//
	    	LOGGER.info("self={}, tick election electionElapsed={}, randomizedElectionTimeout={}", 
	    			r.id, r.electionElapsed, r.randomizedElectionTimeout);
			
			// 如果可以被提升为leader，同时选举时间也到了
			r.electionElapsed = 0;
			//
			// 发送HUP消息是为了重新开始选举
			Message msg = Message.newBuilder()
					.setTo(Const.None)
					.setFrom(r.id)
					.setMsgType(MessageType.MsgHup)
					.build();
			r.step(msg);
		}
    }
}
