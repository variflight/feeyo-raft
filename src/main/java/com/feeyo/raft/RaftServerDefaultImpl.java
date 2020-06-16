package com.feeyo.raft;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.config.RaftConfig;
import com.feeyo.raft.proto.Raftpb.*;
import com.feeyo.raft.storage.MemoryStorage;
import com.feeyo.raft.util.Util;

//
public class RaftServerDefaultImpl extends RaftServer {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftServerDefaultImpl.class );
	
	public RaftServerDefaultImpl(RaftConfig raftCfg, StateMachineAdapter stateMachine) {
		super(raftCfg, stateMachine);
		this.storage = new MemoryStorage();
	}
	
	@Override
	public void start(int reactorSize) throws Throwable {
		super.start( reactorSize );
	}
	
	@Override
	public void stop() {
		super.stop();
	}
	
	//
	@Override
    protected void onNewReady(Ready ready) throws RaftException {
		//
		long beginMillis = TimeUtil.currentTimeMillis();
		//
        // 判断 snapshot 是不是空的，如果不是，那么表明当前节点收到了一个 Snapshot，我们需要去应用这个 snapshot
		final SnapshotMetadata snapshotMeta = ready.unstableSnapshotMeta;
        if ( snapshotMeta != null && snapshotMeta.getIndex() > 0 ) {
        	//
            long snapshotIndex = snapshotMeta.getIndex();
            long appliedIndex = raft.getRaftLog().getApplied();
            if( snapshotIndex <= appliedIndex  ) 
                throw new Errors.RaftException("snapshot index ["+ snapshotIndex + "] should > progress.appliedIndex ["+ appliedIndex +"] + 1)");
            //
            // Apply unstable snapshot to state machine
            applySnapshotToStateMachine( snapshotMeta );
            //
            LOGGER.info("apply snapshot at index {}, old appliedIndex={}", snapshotIndex, appliedIndex);
            raftStatistics.update( RaftStatistics.APPLY_SNAPSHOT, TimeUtil.since( beginMillis ) );
        }
		//
		// Write hardState & entries to log
        long tmpBeginMillis = TimeUtil.currentTimeMillis();
        this.wal.save( ready.unstableEntries, ready.hs );
        raftStatistics.update( RaftStatistics.APPEND_LOGS, TimeUtil.since( tmpBeginMillis ) );
        //
        //
        // 判断 entries 是不是空的，如果不是，表明现在有新增的 entries，我们需要将其追加到 Raft Log 上面
        // 判断 hs 是不是空的，如果不是，表明该节点 HardState 状态变更了，可能是重新给一个新的节点 vote，也可能是 commit index 变了，但无论怎样，我们都需要将变更的 HardState 持续化
        tmpBeginMillis = TimeUtil.currentTimeMillis();
        raft.getRaftLog().getStorage().append( ready.unstableEntries );
        raft.getRaftLog().getStorage().setHardState( ready.hs );
        raftStatistics.update( RaftStatistics.APPEND_STORAGE, TimeUtil.since( tmpBeginMillis ) );
        //
        //
		// 判断是否有  messages，如果有，表明需要给其他 Raft 节点发送消息
        tmpBeginMillis = TimeUtil.currentTimeMillis();
        if ( ready.messages != null && !ready.messages.isEmpty() ) {
        	for(List<Message> msgs : ready.messages.values() ) {
        		this.bcastMessages( msgs );
        	}
        }
        raftStatistics.update( RaftStatistics.BCAST_MSG, TimeUtil.since( tmpBeginMillis ) );
        
        //
		//
        // 判断 committedEntries 是不是空的，如果不是，表明有新的 Log 已经被提交，我们需要去应用这些 Log 到状态机上面了
        // 当然，在应用的时候，也需要保存 apply index
        tmpBeginMillis = TimeUtil.currentTimeMillis();
        List<Entry> applyEntries = entriesToApply( ready.committedEntries );
		applyEntriesToStateMachine( applyEntries );
		raftStatistics.update( RaftStatistics.APPLY_ENTRIES, TimeUtil.since( tmpBeginMillis ) );
		
		//
    	// NewReady Metric
		raftStatistics.update( RaftStatistics.ON_READY, TimeUtil.since(beginMillis) );

        // Maybe trigger local snapshot
        this.maybeTriggerLocalSnapshot();
        
        // Commit ready
        this.commitReady(ready);
    }
	
	private void commitReady(Ready ready) throws RaftException {
		//
		if (ready.ss != null)
			this.prevSs = ready.ss;
		if (ready.hs != null)
			this.prevHs = ready.hs;
		//
		if ( Util.isNotEmpty( ready.unstableEntries ) ) {
			Entry lastEntry = ready.unstableEntries.get( ready.unstableEntries.size() - 1 );
			raft.getRaftLog().stableTo(lastEntry.getIndex(), lastEntry.getTerm());
		}
		//
		if (ready.unstableSnapshotMeta != null) {
			long snapshotIndex = ready.unstableSnapshotMeta.getIndex();
			raft.getRaftLog().stableSnapTo( snapshotIndex );
			raft.getRaftLog().appliedTo( snapshotIndex );
		}
	}
	
	//
    private void bcastMessages(List<Message> messages)  {
    	
    	if (  Util.isNotEmpty( messages ) ) {
    		// metric counter
			for(Message msg: messages)
				raftStatistics.inc(msg);
    		// batch
    		transportClient.syncBatchPost( messages );
    	}
    }
}