package com.feeyo.raft;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;

//TickHeartbeat 是 leader 的 tick 函数
//
// 在超过选举时间时，如果当前打开了raft.checkQuorum开关，那么leader将给自己发送一条MsgCheckQuorum消息
// 对该消息的处理是：检查集群中所有节点的状态，如果超过半数的节点都不活跃了，那么leader也切换到follower状态
public class TickHeartbeat {
	//
    public static void tick(Raft r) throws RaftException {
    	
    	// LoggerUtil.debug("self={}, tick heartbeat heartbeatElapsed={}, electionElapsed={}, heartbeatTimeout={}, electionTimeout={}",
        //			r.id, r.heartbeatElapsed, r.electionElapsed, r.heartbeatTimeout, r.electionTimeout );
    	
        r.heartbeatElapsed++;
        r.electionElapsed++;

        if (r.electionElapsed >= r.electionTimeout) {
            r.electionElapsed = 0;
            
            //
            if (r.checkQuorum) {
            	//用于leader检查集群可用性的消息
            	Message msg = Message.newBuilder()
            			.setTo(Const.None)
            			.setFrom(r.id)
            			.setMsgType(MessageType.MsgCheckQuorum).build();
            	
                r.step( msg );
            }
            
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (r.state == StateType.Leader && r.leadTransferee != Const.None) {
            	// 当前在迁移leader的流程，但是过了选举超时新的leader还没有产生，那么旧的leader重新成为leader
                r.abortLeaderTransfer();
            }
        }

        // 不是leader的就不用往下走了
        if (r.state != StateType.Leader) {
            return;
        }

        // 尝试发送MsgBeat消息
        if (r.heartbeatElapsed >= r.heartbeatTimeout) {
            r.heartbeatElapsed = 0;
            
            // set term ???
            Message msg = Message.newBuilder()
            		.setFrom(r.id)
            		.setMsgType(MessageType.MsgBeat)
            		.build();

            r.step( msg );
        }
    }
}
