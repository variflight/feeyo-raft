package com.feeyo.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;

// Ready encapsulates the entries and messages that are ready to read, be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
//
public class Ready {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( Ready.class );
	
	public SoftState ss;							// 软状态，存储了节点的状态，不要去持久化
	public HardState hs;							// 硬状态，存储了 term commit and vote, 需要持久化
	public List<Entry> unstableEntries;				// 需要在 messages 发送之前存储到 Storage
	public SnapshotMetadata unstableSnapshotMeta;	// 如果 snapshot 不是 empty，则需要存储到 Storage.
	public List<Entry> committedEntries;			// 已经被 committed 的 raft log，可以 apply 到 State Machine 了
	public HashMap<Long,List<Message>> messages;	// 待发送给其他节点的消息，需要在 entries 保存成功之后才能发送，但对于 leader来说，可以先发送 messages，在进行 entries 的保存

	//
	// TODO : 此处待优化，该值应该是对应每个节点来说，而不应该是全量
	public static final int MAX_BATCH_SIZE = (int) Math.round( 3 * 1024 * 1024 ); // 3MB
	public static final int MAX_BATCH_COUNT = 30000;
	
	public Ready(Raft raft, SoftState prevSs, HardState prevHs, long sinceIndex) throws RaftException {

		if ( !raft.getMsgs().isEmpty() ) {
			//
			this.messages = new HashMap<Long, List<Message>>();
			//
			// 攒批
			long batchSize = 0;
			int batchCount = 0;
			
			while( !raft.getMsgs().isEmpty() ) {
				//
				Message msg =  raft.getMsgs().poll();
				if ( msg == null )
					break;
				//
				if ( msg.getTo() != Const.None && msg.getTo() != raft.getId() ) {
					//
					List<Message> toMsgs = messages.get( msg.getTo() );
					if ( toMsgs == null ) {
						toMsgs = new ArrayList<Message>();
						messages.put(msg.getTo(), toMsgs);
					}
					toMsgs.add( msg );
					//
					batchSize += msg.getSerializedSize();
					batchCount++;
				}
				
				//
				if ( batchSize > MAX_BATCH_SIZE || batchCount > MAX_BATCH_COUNT ) {
					LOGGER.info("Batching raft messages, size {}, count {} ", batchSize, batchCount);
					break;
				}
			}
		}
		
		//
		this.ss = raft.isChangedSoftState(prevSs) ? raft.getSoftState() : prevSs;
		this.hs = raft.isChangedHardState(prevHs) ? raft.getHardState() : prevHs;
		//
		this.unstableSnapshotMeta = raft.getRaftLog().getUnstableSnapshotMetadata();	
		this.unstableEntries = raft.getRaftLog().unstableEntries();
		this.committedEntries = raft.getRaftLog().nextEntriesSince( sinceIndex );
	}
	
	// appliedCursor extracts from the Ready the highest index the client has
	// applied (once the Ready is confirmed via commitReady). If no information is contained in the Ready, returns zero.
	public long appliedCursor() {
		if ( this.committedEntries != null ) {
			int n = this.committedEntries.size();
			return this.committedEntries.get(n-1).getIndex();
		}
		
		if ( unstableSnapshotMeta != null && unstableSnapshotMeta.getIndex() > 0  ) {
			return unstableSnapshotMeta.getIndex();
		}
		return 0;
	}	
}
