package com.feeyo.raft;

import java.util.List;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Newdbpb;

//
// The StateMachine interface is supplied by the application to persist/snapshot data of application
public interface StateMachine {

	// committed
	void initialize(long committed);

	//
	// entries & member 
	boolean apply(byte[] data, long committed);
	void applyMemberChange(PeerSet peerSet, long committed);
	
	//
	// snapshot
	void applySnapshot(boolean toCleanup, byte[] data);
	void takeSnapshot(final SnapshotHandler handler) throws RaftException;
	//
	void leaderChange(long leaderId);
	void leaderCommitted(long leaderCommitted);
	
	//
	// The client returns snapshot data here
	public static abstract class SnapshotHandler {
		// 
		@Deprecated
		boolean isDelta = false;
		
		public SnapshotHandler() {}
		
		@Deprecated
		public SnapshotHandler(boolean isDelta) {
			this.isDelta = isDelta;
		}

		public abstract void handle(byte[] data, long seqNo, boolean last) 
				throws RaftException;

		/*
		 * Delta negotiation for snapshot 
		 * 
		 * @param allCfh
		 * @return  true 增量， false 需要全量
		 * @throws RaftException
		 */
		@Deprecated
		public boolean handleDeltaNegotiation(List<Newdbpb.ColumnFamilyHandle> allCfh) throws RaftException {
			if ( !isDelta )
				throw new RaftException("Please take a full snapshot.");
			return true;
		}
		
		//
		@Deprecated
		public void handleDelta(Newdbpb.ColumnFamilyHandle cfh, byte[] data, long seqNo, boolean last) throws RaftException {
			if ( !isDelta )
				throw new RaftException("Please take a full snapshot.");
		}
		
		@Deprecated
		public void handleDeltaEnd() throws RaftException {
			if ( !isDelta )
				throw new RaftException("Please take a full snapshot.");
		}
	}

}
