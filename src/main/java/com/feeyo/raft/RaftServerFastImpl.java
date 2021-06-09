package com.feeyo.raft;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.config.RaftConfig;
import com.feeyo.raft.proto.Raftpb.*;
import com.feeyo.raft.storage.FileStorage;
import com.feeyo.raft.util.Util;

/**
 * TODO: Jepsen test
 * 
 * @author zhuam
 */
public class RaftServerFastImpl extends RaftServer {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftServerFastImpl.class );

	public RaftServerFastImpl(RaftConfig raftConfig, StateMachineAdapter stateMachine) {
		super(raftConfig, stateMachine);
		this.storage = new FileStorage(storageDir);
	}

	@Override
	public void start(int reactorSize) throws Throwable {
		super.start(reactorSize);
	}
	
	@Override
	public void stop() {
		super.stop();
	}
	//
	private void bcastMessages(List<Message> messages) {
		if (Util.isEmpty(messages))
			return;
		//
		// metric counter
		for(Message msg: messages)
			raftStatistics.inc(msg);
		//
		// pipeline && batch
		transportClient.pipeliningBatchPost(messages);
	}
	//
	@Override
    protected void onNewReady(final Ready ready) throws RaftException {
		//
		// @see https://zhuanlan.zhihu.com/p/25735592
		//
		// Optimization
		// 1、Batch and Pipeline
		// 2、Append Log Parallelly
		// 3、Asynchronous Apply
		// 4、Asynchronous Lease Read
		//
		long beginMillis = TimeUtil.currentTimeMillis();
		//
		int parallelNum = 1;  	// Asynchronous Apply
		boolean isLeader = raft.isLeader();
		if ( isLeader ) {
			parallelNum += ready.messages != null ? ready.messages.size() + 1 : 1; // Send Message Parallelly + Append Log Parallelly
		} else  {
			parallelNum += 1; // Follower Parallelly
		}
		//
		final CountDownLatch latch = new CountDownLatch( parallelNum );
		//
		// for leader
		if (isLeader) {
			//
	    	// Batch and Pipeline
			if ( ready.messages != null && !ready.messages.isEmpty()) {
				//
				for (final List<Message> msgs: ready.messages.values()) {
					this.dispatchTpExecutor.execute(new Runnable() {
						@Override
						public void run() {
							long bcastBeginMillis = TimeUtil.currentTimeMillis();
							try {
								bcastMessages(msgs);
							} catch (Throwable e) {
								LOGGER.error("broadcast msg err:", e);
							} finally {
								long elapsed = TimeUtil.since(bcastBeginMillis);
								if (elapsed > 50 && !Util.isNotEmpty(msgs)) {
									LOGGER.info("broadcast slow, id={}, msgs={}, elapsed={}", msgs.get(0).getTo(), msgs.size(), elapsed );
								}
								//
								raftStatistics.update(RaftStatistics.BCAST_MSG, elapsed);
								latch.countDown();
							}
						}
					});
				}
			}
			//
	    	// Append Log Parallelly
	    	this.dispatchTpExecutor.execute( new Runnable(){
	    		@Override
	    		public void run() {
	    			long tmpBeginMillis = TimeUtil.currentTimeMillis();
	    			try {
	    				// Write hardState & entries to log
						wal.save(ready.unstableEntries, ready.hs);
	    		        raftStatistics.update(RaftStatistics.APPEND_LOGS, TimeUtil.since(tmpBeginMillis));
	    		        tmpBeginMillis = TimeUtil.currentTimeMillis();

	    		        // Write entries to local raftStorage
						raft.getRaftLog().getStorage().append(ready.unstableEntries);
						raft.getRaftLog().getStorage().setHardState(ready.hs);
	    		        raftStatistics.update(RaftStatistics.APPEND_STORAGE, TimeUtil.since(tmpBeginMillis));
	    			} catch (Throwable e) {
	    				LOGGER.error("append log parallelly err:", e);
	    			} finally {
	    				latch.countDown();
	    			}
	    		}
	    	});
	    	
		} else {
			//
			// Follower Parallelly
			this.dispatchTpExecutor.execute( new Runnable(){
	    		@Override
	    		public void run() {
	    			try {
	    				//
						// TODO: 注意事项
						// Apply snapshot 这个地方会造成阻塞，很大可能会触发新的选举, 后续优化
						final SnapshotMetadata snapshotMeta = ready.unstableSnapshotMeta;
				        if ( snapshotMeta != null && snapshotMeta.getIndex() > 0 ) {
				        	long tmpBeginMillis = TimeUtil.currentTimeMillis();
				            long snapshotIndex = snapshotMeta.getIndex();
				            long appliedIndex = raft.getRaftLog().getApplied();
				            if( snapshotIndex <= appliedIndex  ) 
				                throw new Errors.RaftException(String.format("snapshot index [%s] should > progress.appliedIndex [%s] + 1)", snapshotIndex, appliedIndex));
				            
				            // Apply snapshot to state machine
							applySnapshotToStateMachine(snapshotMeta);
				            LOGGER.info("apply snapshot at index {}, old appliedIndex={}", snapshotIndex, appliedIndex);
				            //
							raft.getRaftLog().stableSnapTo(snapshotIndex);
							raft.getRaftLog().appliedTo(snapshotIndex);
				            raftStatistics.update( RaftStatistics.APPLY_SNAPSHOT, TimeUtil.since( tmpBeginMillis ) );
				        }
				        //
				        // ---------------------------------------------------------------------
				    	// Write hardState & unstable entries to log
				        long tmpBeginMillis = TimeUtil.currentTimeMillis();
						wal.save(ready.unstableEntries, ready.hs);
				        raftStatistics.update(RaftStatistics.APPEND_LOGS, TimeUtil.since(tmpBeginMillis));
				        //
				        // Write unstable entries to local raftStorage 
				        tmpBeginMillis = TimeUtil.currentTimeMillis();
						raft.getRaftLog().getStorage().append(ready.unstableEntries);
						raft.getRaftLog().getStorage().setHardState(ready.hs);
				        raftStatistics.update(RaftStatistics.APPEND_STORAGE, TimeUtil.since(tmpBeginMillis));
				        //
				        // ---------------------------------------------------------------------
				    	// broadcast message
				        if ( ready.messages != null && !ready.messages.isEmpty() ) {
				        	tmpBeginMillis = TimeUtil.currentTimeMillis();
				        	for(List<Message> msgs: ready.messages.values() ) 
				        		bcastMessages( msgs );
				        	//
				        	raftStatistics.update(RaftStatistics.BCAST_MSG, TimeUtil.since(tmpBeginMillis));
				        }
				        
	    			} catch (Throwable e) {
	    				LOGGER.error("follower parallelly err:", e);
	    			} finally {
	    				latch.countDown();
	    			}
	    		}
	    	});
		}
		//
		// ---------------------------------------------------------------------
		// Asynchronous Apply
		this.dispatchTpExecutor.execute( new Runnable(){
    		@Override
    		public void run() {
    			try {	
					List<Entry> committedEntries = ready.committedEntries;
					if ( Util.isEmpty( committedEntries ) )
						return;
					//
					long applyBeginMillis = TimeUtil.currentTimeMillis();
					List<Entry> applyEntries = entriesToApply(committedEntries);
					applyEntriesToStateMachine(applyEntries);
					//
					raftStatistics.gauge(raft.getRaftLog().getCommitted(), raft.getRaftLog().getApplied());
					raftStatistics.update(RaftStatistics.APPLY_ENTRIES, TimeUtil.since(applyBeginMillis));
					
    			} catch (Throwable e) {
    				LOGGER.error("apply entries parallelly err:", e);
    			} finally {
    				latch.countDown();
    			}
    		}
    	});
		
		//
    	try {
			latch.await();
		} catch (InterruptedException e) {
			/* ignore */
		}
    	
    	// NewReady Metric
    	raftStatistics.update( RaftStatistics.ON_READY, TimeUtil.since(beginMillis) );
		
        // Maybe trigger local snapshot
        this.maybeTriggerLocalSnapshot();
        
        // Commit ready
        this.commitReady(ready);
    }
	
	private void commitReady(Ready ready) throws RaftException {
 		if (ready.ss != null)
 			this.prevSs = ready.ss;

 		if (ready.hs != null)
 			this.prevHs = ready.hs;
 		//
		if (Util.isNotEmpty(ready.unstableEntries)) {
			Entry lastEntry = ready.unstableEntries.get(ready.unstableEntries.size() - 1);
			raft.getRaftLog().stableTo(lastEntry.getIndex(), lastEntry.getTerm());
 		}	
	}
}