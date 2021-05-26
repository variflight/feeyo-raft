package com.feeyo.raft.caller;

import java.util.List;


import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Message;

public abstract class RaftCallerAdapter<M> implements RaftCaller<M> {
	//
	// 获取 callback key
	protected String getCbKey(Message msg) {
		List<Raftpb.Entry> entries = msg.getEntriesList();
		int lastEntryIndex  = entries.size() - 1;
		return entries.get( lastEntryIndex  ).getCbkey();
	}
	
	//
	// 注入 callback key
	protected Message injectKeyToMsg(String key, Message msg) {
		// TODO: 
		// 当前用 last entry 的 data 做为 propose 回调的 key ，并不是最好的选择
		// leader wait && follower forward to leader and wait
		//
		List<Raftpb.Entry> entries = msg.getEntriesList();
		int lastEntryIndex  = entries.size() - 1;
		
		Raftpb.Entry lastEntry = entries.get( lastEntryIndex )	//
				.toBuilder()	//
				.setCbkey( key )	//
				.build();	//
		//
		return msg.toBuilder()	//
				.setEntries(lastEntryIndex, lastEntry)	//
				.build();	//
	}
}