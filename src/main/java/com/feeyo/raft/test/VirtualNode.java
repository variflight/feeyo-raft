package com.feeyo.raft.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.Config;
import com.feeyo.raft.Const;
import com.feeyo.raft.LinearizableReadOption;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.Raft;
import com.feeyo.raft.RaftNodeAdapter;
import com.feeyo.raft.ReadOnlyOption;
import com.feeyo.raft.StateType;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.config.RaftConfig;
import com.feeyo.raft.proto.Raftpb.ConfChange;
import com.feeyo.raft.proto.Raftpb.ConfChangeType;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.storage.MemoryStorage;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.StandardThreadExecutor;
import com.feeyo.raft.util.Util;
import com.google.protobuf.ByteString;

public class VirtualNode extends RaftNodeAdapter {

	// ..
	public static Map<String, SyncWaitCallback> waitingCallbacks = new ConcurrentHashMap<>();
	public static StandardThreadExecutor threadPoolExecutor  = new StandardThreadExecutor(20, 100, 10, new NamedThreadFactory("raftN/threadPool-", true));
 	
	//
	// Raft tick interval
	private static final int TICK_TIMEOUT = 100;
	//
	private Peer peer;
	private PeerSet peerSet;
	
	private Raft raft;
	private Thread raftThread;
	
	private AtomicBoolean isClosed = new AtomicBoolean(true);
	
	private VirtualRaftCluster cluster;
	
	public VirtualNode(Peer peer, PeerSet peerSet, VirtualRaftCluster cluster) {
		this.peer = peer;
		this.peerSet = peerSet;
		//
		this.cluster = cluster;
		this.cluster.nodeSet.put(peer.getId(), this);
	}
	
	//
	private Raft createRaft() throws RaftException {
		Config c = new Config();
		c.setElectionTick(50);
		c.setHeartbeatTick(10);
		c.setApplied( 0 );
		c.setMaxSizePerMsg( 1024 * 1024 );
		c.setMaxInflightMsgs(256);
		c.setMinElectionTick(0);
		c.setMaxElectionTick(0);
		c.setMaxLogFileSize(10 * 1024 * 1024);
		c.setSnapCount(1000);
		c.setCheckQuorum(true);
		c.setPreVote(true);
		c.setSkipBcastCommit(false);
		c.setStorageDir( "" );
		c.setReadOnlyOption( ReadOnlyOption.Safe );
		c.setLinearizableReadOption( LinearizableReadOption.FollowerRead );
		c.setDisableProposalForwarding( false );
		
		// set local
		RaftConfig cfg = new RaftConfig(c);
		cfg.setLocalPeer( peer );
		cfg.setPeerSet( peerSet );
		
        this.targetPriority = super.getMaxPriorityOfNodes();
		
		//
		Raft raft = new Raft(c, new MemoryStorage(), this);
		
		long lastIndex = raft.getRaftLog().getStorage().lastIndex();
		if ( lastIndex == 0 ) {
			raft.becomeFollower(1, Const.None);
			//
			List<Entry> entries = new ArrayList<Entry>();
			//
			for(Peer peer: cfg.getPeerSet().values()) {
				ConfChange cs = ConfChange.newBuilder() //
						.setChangeType(ConfChangeType.AddNode) //
						.setNodeId( peer.getId() ) //
						.setContext(ByteString.copyFromUtf8( peer.getEndpoint().toString())) //
						.build(); //
				
				Entry entry = Entry.newBuilder() //
						.setEntryType(EntryType.EntryConfChange) //
						.setData( ByteString.copyFrom( cs.toByteArray() ) ) //
						.build(); //
				entries.add( entry );
			}
			
			raft.getRaftLog().append(raft.getTerm(), entries);
			raft.getRaftLog().commitTo( entries.size() ); // TODO ???
			
			//
			for (Peer peer: peerSet.values()) {
				raft.addVoterOrLearner(peer.getId(), peer.isLearner());
			}
		}
		return raft;
	}
	
	
	public void start() throws RaftException {
		//
		if ( !this.isClosed.compareAndSet(true, false) ) {
			return;
		}
		//
		this.raft = createRaft();
		//
		this.raftThread = new Thread() {
			public void run() {
				//
				long startMs = System.currentTimeMillis();
				
				while ( !isClosed.get() ) {
					try {
						// Tick
						long currentMs = System.currentTimeMillis();
						if (currentMs - startMs > TICK_TIMEOUT) {
							startMs = currentMs;
							// internal logical clock by a single tick
							raft.tick();
						}
						
						// Has ready
						long sinceIndex = raft.getRaftLog().getApplied();
						boolean hasMsg = !raft.getMsgs().isEmpty();
						boolean hasUnstable = raft.getRaftLog().hasUnstableEntries();
						boolean hasCommit = raft.getRaftLog().hasNextEntriesSince( sinceIndex );
						if (hasMsg || hasUnstable || hasCommit) {
							
							//
//							System.out.println( "ccc=" + System.currentTimeMillis()  
//									+ ", sinceIndex= "+ sinceIndex 
//									+ ", hasMsg=" + raft.getMsgs().size()
//									+ ", hasUnstable=" + hasUnstable
//									+ ", hasCommit=" + hasCommit);
							
							
							// unstable to storage
							List<Entry> unstableEntries = raft.getRaftLog().unstableEntries();
							if ( unstableEntries != null && !unstableEntries.isEmpty() ) {
								//
								raft.getRaftLog().getStorage().append( unstableEntries );
								
								Entry lastE = unstableEntries.get( unstableEntries.size() - 1);
								raft.getRaftLog().stableTo(lastE.getIndex(), lastE.getTerm());
							}
							
							// commit to apply
							List<Entry> committedEntries = raft.getRaftLog().nextEntriesSince( sinceIndex );
							if ( committedEntries != null && !committedEntries.isEmpty() ) {
								Entry laste = null;
								for(Entry e: committedEntries ) {
									laste = e;
									
									 if ( StringUtils.isNotEmpty( e.getCbkey()) ) {
										 //
										SyncWaitCallback c = waitingCallbacks.remove( e.getCbkey() );
										if ( c != null )
											c.onCompleted();
									 }
									
								}
								raft.getRaftLog().appliedTo( laste.getIndex() );
							}
							
							//
							ConcurrentLinkedQueue<Message> msgQueue = raft.getMsgs();
								
							while ( !msgQueue.isEmpty() ) {
								//
								if ( isClosed.get() )
									break;
	
								final Message message = msgQueue.poll();
								if ( message != null ) {
									//threadPoolExecutor.execute( new Runnable(){
										//@Override
										//public void run() {
											VirtualNode node = cluster.nodeSet.get( message.getTo() );
											if ( node != null )
												try {
													
													System.out.println( ProtobufUtils.protoToJson( message ) );
													
													node.raft.step( message );
												} catch (RaftException e) {
													e.printStackTrace();
												}
									//	}
									//});
								}
							}
							
							
						} else {
							Util.waitFor(1L); 	// 5 Millisecond
							continue;
						}
						
						//
					} catch (Throwable e) {
						//
					}
				}
			}
		};
		raftThread.start();
		
	}
	
	public void stop() {
		//
		if ( !this.isClosed.compareAndSet(false, true)) {
			return;
		}
		try {
			this.raftThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void onStateChange(long id, StateType newStateType, long leaderId) {
		//
		cluster.leaderId = leaderId;
		System.out.println("$$$$$$$$$$$$$ id=" + id + ",  leaderId=" + leaderId);
	}

	@Override
	public void onReadIndex(String rctx, long index) {
		// ignore
	}
	
	@Override
	public void onAppliedIndex(long appliedIndex) {
		// ignore
	}

	@Override
	public void onReceivedHeartbeat(Message msg) {
		//
	}

	@Override
	public void onProposalForwarding(Message msg) {
		//
	}

	@Override
	public void onSendSnapshots(Message msg) {
		//
	}

	public void syncWait(final Message message, final SyncWaitCallback c) {
		if (raft != null) {

			threadPoolExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						waitingCallbacks.put(c.cbKey, c);

						raft.step(message);
					} catch (RaftException e) {
						e.printStackTrace();
					}
				}
			});
		}
	}
	
	//
	public static class SyncWaitCallback {
		
		final Object monitor = new Object();
		boolean isNotified = false;
		
		public String cbKey;
		
		public void onCompleted() {	
			//
			synchronized( monitor ) {
				isNotified = true;
				monitor.notify();
			}
		}
		
		public void await() {
			synchronized( monitor ) {
				if ( !isNotified ) {
					try {
						monitor.wait();
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
		}
		
	}

	public long getId() {
		return peer.getId();
	}


	@Override
	public Peer getPeer() {
		return this.peer;
	}

	@Override
	public PeerSet getPeerSet() {
		return this.peerSet;
	}

}
