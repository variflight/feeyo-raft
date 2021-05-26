package com.feeyo.raft.proto.util;

import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.ConfChange;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.google.protobuf.ByteString;

public final class MessageUtil {
	//
	// ReadIndex requests a read state. The read state will be set in ready.
	// Read State has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	//
	public static Message readIndex(byte[] rctx) {
		//
		Raftpb.Entry entry = Raftpb.Entry.newBuilder() //
				.setEntryType(Raftpb.EntryType.EntryNormal) //
				.setData(ByteString.copyFrom(rctx)) //
				.build(); //

		Raftpb.Message msg = Raftpb.Message.newBuilder() //
				.setMsgType(Raftpb.MessageType.MsgReadIndex) //
				.addEntries(entry) //
				.build(); //
		
		return msg;
	}
	
	// ProposeConfChange proposes a config change.
	public static Message proposeConfChange(ConfChange cc)  {
		//
		Entry entry = Entry.newBuilder() //
				.setEntryType(EntryType.EntryConfChange) //
				.setData( ByteString.copyFrom( cc.toByteArray() ) ) //
				.build(); //
		Message msg = Message.newBuilder() //
				.setMsgType(MessageType.MsgPropose) //
				//.setFrom(1000) //
				.addEntries( entry ) //
				.build(); //
		return msg;
	}
	
	// TransferLeader tries to transfer leadership to the given transferee.
	public static Message transferLeader(long transferee)  {
		Message msg = Message.newBuilder() //
				.setMsgType(MessageType.MsgTransferLeader) //
				.setFrom(transferee) //
				.build(); //
		return msg;
	}
	
	// 仅leader才处理这类消息，leader如果判断该节点此时处于正常接收数据的状态(ProgressState.Replicate)，那么就切换到探测状态
	//
	// ReportUnreachable reports the given node is not reachable for the last send.
	public static Message reportUnreachable(long id) {
		Message msg = Message.newBuilder() //
				.setMsgType(MessageType.MsgUnreachable) //
				.setFrom(id) //
				.build(); //
		return msg;
	}
	
	// ReportSnapshot reports the status of the sent snapshot.
	public static Message reportSnapshot(long id, boolean reject) {
		Message msg = Message.newBuilder() //
				.setMsgType(MessageType.MsgSnapStatus) //
				.setFrom(id) //
				.setReject( reject ) //
				.build(); //
		return msg;
	}
}