package com.feeyo.raft;

import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.caller.RaftCaller;
import com.feeyo.raft.caller.RaftCallerImpl;
import com.feeyo.raft.caller.callback.Callback;
import com.feeyo.raft.caller.callback.CallbackRegistry;
import com.feeyo.raft.config.RaftConfig;
import com.feeyo.raft.proto.Newdbpb;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.*;
import com.feeyo.raft.proto.util.MessageUtil;
import com.feeyo.raft.storage.Storage;
import com.feeyo.raft.storage.snapshot.DefaultSnapshotter;
import com.feeyo.raft.storage.snapshot.AbstractSnapshotter;
import com.feeyo.raft.storage.snapshot.DeltaSnapshotter;
import com.feeyo.raft.storage.wal.Wal;
import com.feeyo.raft.transport.AbstractTransportClient;
import com.feeyo.raft.transport.TransportClient;
import com.feeyo.raft.transport.TransportServer;
import com.feeyo.raft.transport.client.AsyncHttpResponseHandler;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.feeyo.raft.util.HashCAS;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.StandardThreadExecutor;
import com.feeyo.raft.util.Util;
//
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroByteStringHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 *<pre><code>    [0]                     [1]                       [2]
 *------------&gt; Follower --------------&gt; Candidate --------------&gt; Leader
 *               ^  ^                       |                        |
 *               |  |         [3]           |                        |
 *               |  |_______________________|                        |
 *               |                                                   |
 *               |                                 [4]               |
 *               |___________________________________________________|
 *
 * - [0] Starts up | Recovers
 * - [1] Times out | Starts election
 * - [2] Receives votes from majority of servers and becomes leader
 * - [3] Discovers leader of new term | Discovers candidate with a higher term
 * - [4] Discovers server with higher term
 *
 *</code></pre>
 *
 * All nodes in the Raft protocol begin in the follower state. <p/>
 * 
 * @author zhuam
 */
@SuppressWarnings("deprecation")
public abstract class RaftServer extends RaftNodeAdapter {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( RaftServer.class );
	//
	public static final int TICK_TIMEOUT = 100;
	//
	protected RaftConfig raftCfg;
	protected String storageDir;
	//
	protected PeerSet peerSet;

	// Loop Thread & ThreadPool
	protected AtomicBoolean running = new AtomicBoolean( false );
	private ThreadFactory threadFactory = new NamedThreadFactory("rf/loop-", true);
	private Thread coreLoopThread;
 	protected StandardThreadExecutor dispatchTpExecutor;
 	protected StandardThreadExecutor notifyTpExecutor;
	//
	protected Raft raft;
	protected Storage storage;
	protected volatile SoftState prevSs;		// 软状态，存储了节点的状态，不要去持久化
	protected volatile HardState prevHs;		// 硬状态，存储了 term commit and vote, 需要持久化
	//
	// WAL & Snapshot
	protected Wal wal;
	private AbstractSnapshotter snapshotter;
	private AtomicBoolean isCreatingSnapshot = new AtomicBoolean(false);
	private long lastSnapshotTime = TimeUtil.currentTimeMillis();
	private HashCAS snapshotSendingCAS = new HashCAS();	 // hash CAS [key= Message.getTo() ]
	//
	private volatile long leaderId = 0;
    protected StateMachineAdapter stateMachine;
	//
	// Net I/O
	private TransportServer transportServer;
	protected AbstractTransportClient<Raftpb.Message> transportClient;
	//
	//
	protected RaftStatistics raftStatistics;
	//
	private RaftCaller<Message> raftCaller;
	private CallbackRegistry callbackRegistry;
	
	//
	@Deprecated
	protected boolean isDeltaSnapshot = false;
	
	//
	protected volatile int targetPriority;			// 目标leader 的选举权重值
	protected volatile int electionTimeoutCounter;	// 当前节点的选举超时数
	
	//
	//
	public RaftServer(RaftConfig raftCfg, StateMachineAdapter stateMachine) {
		this.raftCfg = raftCfg;
		this.storageDir = raftCfg.getCc().getStorageDir() + File.separator;
		this.peerSet = raftCfg.getPeerSet();
		this.stateMachine = stateMachine;
		//
		// initially set to max(priority of all nodes)
        this.targetPriority = getMaxPriorityOfNodes();
        this.electionTimeoutCounter = 0;
		//
		// Asynchronous thread pool  dispatch & notify
		int coreThreads = Math.max(this.raftCfg.getTpCoreThreads() / 2, 4);
		int maxThreads = Math.max(this.raftCfg.getTpMaxThreads(), coreThreads);
		int queueCapacity = Math.min(this.raftCfg.getTpQueueCapacity(), 1500);
		//
		this.dispatchTpExecutor = new StandardThreadExecutor(coreThreads, maxThreads, queueCapacity, new NamedThreadFactory("rf/dispatch-"));
		this.dispatchTpExecutor.prestartAllCoreThreads();
		//
		this.notifyTpExecutor = new StandardThreadExecutor(coreThreads, maxThreads, queueCapacity, new NamedThreadFactory("rf/notify-"));
		this.notifyTpExecutor.prestartAllCoreThreads();
		//
		this.raftStatistics = new RaftStatistics();
		//
		this.raftCaller = new RaftCallerImpl(this);
		this.callbackRegistry = new CallbackRegistry( raftStatistics );
	}
	
	//
	@Deprecated
	public void setDeltaSnapshot(boolean isDelta) {
		this.isDeltaSnapshot = isDelta;
	}

	//
	public void start(int reactorSize) throws Throwable {
		//
		if ( !running.compareAndSet(false , true) )
			return;

		LOGGER.info("------------------------------ raft ------------------------------");
		//
		try {
			// 1、Loading snapshot metadata
			this.snapshotter = isDeltaSnapshot ? new DeltaSnapshotter(storageDir) : new DefaultSnapshotter(storageDir);
			SnapshotMetadata snapshotMeta = snapshotter.getMetadata();
			LOGGER.info("self={}, replay snapshot, meta={} ", getId(), ProtobufUtils.protoToJson(snapshotMeta));
			//
			// 2、Replay WAL log files
			this.wal = new Wal(storageDir, raftCfg.getCc().getMaxLogFileSize(), raftCfg.getCc().isSyncLog());
			Wal.ReadAllLogs walLogs = this.wal.readAll( snapshotMeta == null ? -1 : snapshotMeta.getIndex() );
			LOGGER.info("self={}, replay wal, hs={}, ents={}", getId(), Util.toStr(walLogs.getHs()), Util.toStr(walLogs.getEntries()));

			//
			// 3、Rebuilding storage
			storage.applySnapshotMetadata( snapshotMeta );
			storage.setHardState( walLogs.getHs() );
			storage.append( walLogs.getEntries() );
			LOGGER.info("self={}, replay storage, firstIdx={}, lastIdx={}", getId(), storage.firstIndex(), storage.lastIndex());
			//
			// 4、Initialize raft
			this.raft = new Raft(raftCfg.getCc(), storage, this);
			final RaftLog raftLog = raft.getRaftLog();
			long lastIndex = raftLog.getStorage().lastIndex();
			if ( lastIndex == 0 ) {
				// If the log is empty
				raft.becomeFollower(1, Const.None);
				//
				List<Entry> entries = new ArrayList<Entry>();
				for(Peer peer: raftCfg.getPeerSet().values()) {
					entries.add( newConfChange(ConfChangeType.AddNode, peer) );
					raft.addVoterOrLearner(peer.getId(), peer.isLearner());		// add voter or learner
				}
				raftLog.append(raft.getTerm(), entries);
				raftLog.commitTo( entries.size() ); // TODO ???
			}
			//
			//
			this.prevSs = raft.getSoftState();
			this.prevHs = (raftLog.getStorage().lastIndex() != 0 ? raft.getHardState(): HardState.getDefaultInstance() );
			this.stateMachine.initialize( raftLog.getCommitted() );
			//
			LOGGER.info("self={}, raftLog={}", getId(), raftLog);
			//
			// 5、Create transport for raft
			this.transportServer = new TransportServer( RaftServer.this );
			this.transportServer.start(raftCfg.getLocal().getPort(), reactorSize);
			//
			this.transportClient = new TransportClient();
			this.transportClient.start( peerSet );
			//
			// 6、Start core & schedule thread
			this.startCoreLoopThread();
			this.startScheduleThread();
			//
			System.out.println("raft startup, port=" + raftCfg.getLocal().getPort() );
			//
		} catch (Throwable e) {
			LOGGER.error("raft init err: ",  e);
			throw e;
		}
	}

	//
	public boolean isRunning() {
		return running.get();
	}

	private void startScheduleThread() {
		//
		// 客户端连接的心跳检测
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						if ( transportClient != null )
							transportClient.check();
					}
				});
			}
		}, 15L, 10L, TimeUnit.SECONDS);

		//
		// Callback 超时检测
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						callbackRegistry.timeoutCheck();
					}
				});
			}
		}, 30L, 20L, TimeUnit.MILLISECONDS);

		//
		// 过期快照文件的清理
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						snapshotter.gc();
					}
				});
			}
		}, 12, 12, TimeUnit.MINUTES);

		//
		// 定时输出 Metric
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						raftStatistics.print("raft");
					}
				});
			}
		}, 5, 5, TimeUnit.MINUTES);
	}

	//
	// 核心循环处理线程
	private void startCoreLoopThread() throws Exception {
		//
		// event loop on raft state machine updates
		this.coreLoopThread = threadFactory.newThread(new Runnable() {
			@Override
			public void run() {
				//
				long startMs = 0; // TimeUtil.currentTimeMillis();
				while (running.get()) {
					try {
						// Tick
						long currentMs = TimeUtil.currentTimeMillis();
						if (currentMs - startMs > TICK_TIMEOUT) {
							startMs = currentMs;
							// internal logical clock by a single tick
							raft.tick();
						}

						//
						// store raft entries to wal, then publish over commit channel
						long sinceIndex = raft.getRaftLog().getApplied();
						if (!raft.getMsgs().isEmpty()
								|| raft.getRaftLog().hasUnstableEntries()
								|| raft.getRaftLog().hasNextEntriesSince( sinceIndex )
								|| raft.getRaftLog().hasUnstableSnapshotMetadata()
								|| raft.isChangedSoftState(prevSs)
								|| raft.isChangedHardState(prevHs)) {
							//
							Ready ready = new Ready(raft, prevSs, prevHs, sinceIndex);
							onNewReady( ready );

						} else {
							Util.waitFor(1L); 	// 1 Millisecond
							continue;
						}

					} catch (Throwable e) {
						// 
						// 忽略 java.util.concurrent.RejectedExecutionException
						if ( e instanceof RejectedExecutionException )
							return;
						//
						running.set( false );
						LOGGER.error("raft coreLoopThread err: " + raft.getRaftLog(), e);
					}
				}
			}
		});
		this.coreLoopThread.start();
	}

	//
	protected abstract void onNewReady(Ready ready) throws RaftException;
	
	//
	public void stop() {
		//
		if ( !running.compareAndSet(true , false) )
			return;
		//
		if ( this.callbackRegistry != null )
			this.callbackRegistry.shutdown();
		//
		if ( this.wal != null )
			this.wal.stop();
		//
		if ( this.storage != null )
			this.storage.close();
		//
		// 依赖 running 标记实现关闭 coreLoopThread
		//
		if ( this.dispatchTpExecutor != null )
			this.dispatchTpExecutor.shutdownNow();

		if ( this.notifyTpExecutor != null )
			this.notifyTpExecutor.shutdownNow();
		//
		if ( this.transportClient != null)
			this.transportClient.stop();

		if( this.transportServer != null )
			this.transportServer.stop();
	}

	//
	// 从 committed entries 中提取可 apply 至状态机的 entries
	protected List<Entry> entriesToApply(List<Entry> ents) throws RaftException {
		//
		if ( Util.isEmpty( ents ) )
			return ents;
		//
        long firstIdx = ents.get(0).getIndex();
        if( firstIdx > raft.getRaftLog().getApplied() + 1)
            throw new Errors.RaftException("first index of committed entry[" + firstIdx
            		+ "] should <= progress.appliedIndex[" + raft.getRaftLog().getApplied() + "]+1");

        // E.g:
        // 1: appliedIdx=13  firstIdx=14  entsSize=10
        //  offset = 0, entries[0-10]
        //
        // 2: appliedIdx=13  firstIdx=11  entsSize=20
        // offset = 3, entries=[3-10]
        //
        int offset = (int) (raft.getRaftLog().getApplied() - firstIdx + 1);
        if( offset < ents.size()) {
        	return Util.slice(ents, offset, ents.size());
        }
        return null;
	}

	//
	// Apply remote snapshot to state machine
	protected void applySnapshotToStateMachine(SnapshotMetadata meta) throws RaftException {
		//
		long index = meta.getIndex();
		long term = meta.getTerm();
		//
		AbstractSnapshotter.Readable snapReadable = null;
		try {
			snapReadable = snapshotter.open(true, index, term);
			while( snapReadable.isValid() ) {
				//
				Snapshot snapshot = snapReadable.next();
				if ( snapshot == null || snapshot.getChunk() == null )
					break;
				//
				String fileName = snapReadable.getFile().getName();
				long seqNo = snapshot.getChunk().getSeqNo();
				boolean last = snapshot.getChunk().getLast();
				ByteString data = snapshot.getChunk().getData();
				if ( data != null )
					stateMachine.applySnapshot( seqNo == 1, ZeroByteStringHelper.getByteArray( data ) );  // 确定业务状态机需要做历史数据的 cleanup
				//
                if ( last ) {
                	LOGGER.warn("apply snapshot, {} {} is completed", fileName, seqNo);
                } else {
                	if (seqNo == 1)
						LOGGER.warn("apply snapshot, {} {} is started", fileName, seqNo);
					else if (seqNo != 0 && seqNo % 100 == 0)
						LOGGER.warn("apply snapshot, {} {}", fileName, seqNo);
                }
			}
            //
		} finally {
			//
			IOUtil.closeQuietly( snapReadable );
			//
			// 转移远程快照目录 至 本地快照目录
			snapshotter.moveFile(snapReadable);
			//
            wal.saveSnapMeta( meta );
            raft.getRaftLog().getStorage().applySnapshotMetadata( meta );
		}
	}

	//
	// Apply entries to state machine
    protected void applyEntriesToStateMachine(List<Entry> entries) throws RaftException {

        if( Util.isEmpty( entries ) )
            return;
        //
        if (LOGGER.isDebugEnabled() )
        	LOGGER.debug("apply entries, ent size={} index={},", entries.size(), entries.get(0).getIndex() );

        for (Entry ent: entries) {

        	//
            if (ent.getData() == null || ent.getData().isEmpty()) {
                // ignore empty messages
            	LOGGER.info("ignore empty messages, ent={} ", ProtobufUtils.protoToJson(ent));

            } else {

	            EntryType entryType = ent.getEntryType();
	            if ( entryType == EntryType.EntryNormal ) {
	            	long idx = ent.getIndex();
	            	byte[] data = ZeroByteStringHelper.getByteArray( ent.getData() );
	            	//
	                boolean isOk = stateMachine.apply(data, idx);
	                if ( StringUtils.isNotEmpty( ent.getCbkey()) )
	                	callbackRegistry.notifyCallbacks(ent.getCbkey(), isOk ? Callback.Action.Commit : Callback.Action.Error);
	                //
				} else if (entryType == EntryType.EntryConfChange) {
					boolean isOk = false;
					try {
						//
						ConfChange cc = ConfChange.parseFrom(ent.getData());
						boolean isChanged = this.applyConfChange(cc);
						if (isChanged) {
							// transportClient 热更新
							if (transportClient != null)
								transportClient.reload( peerSet );
							//
							// 通知Raft内部 progress 信息改变
							raft.applyConfChange(cc);
							//
							// 通知状态机，成员改变
							this.stateMachine.applyMemberChange(peerSet, ent.getIndex());
							LOGGER.info("#ConfChange, entry= {}", ProtobufUtils.protoToJson(ent));
						}

						isOk = isChanged;

					} catch (InvalidProtocolBufferException e) {
						LOGGER.error("ConfChange err:", e);
					} finally {

						// propose callback
						if (StringUtils.isNotEmpty(ent.getCbkey()))
							callbackRegistry.notifyCallbacks(ent.getCbkey(), isOk ? Callback.Action.Commit : Callback.Action.Error);
					}
				}
            }
            //
            // after commit, update appliedIndex
            long newAppliedIndex = ent.getIndex();

            // LOGGER.info("after commit, applied={}", newAppliedIndex );
            raft.getRaftLog().setApplied( newAppliedIndex );
        }
    }

    //
    //
	private Entry newConfChange(ConfChangeType type, Peer peer) {
		//
		ConfChange cc = ConfChange.newBuilder()
				.setChangeType(type)	//
				.setNodeId(peer.getId())	//
				.setContext(ByteString.copyFromUtf8(peer.getEndpoint().toString())) //
				.build(); //
		Entry entry = Entry.newBuilder()	//
				.setEntryType(EntryType.EntryConfChange)	//
				.setData(ZeroByteStringHelper.wrap( cc.toByteArray() )).build(); //
		return entry;
	}

	//
	private boolean applyConfChange(ConfChange cc) {
		//
		boolean isChanged = false;
		//
		long id = cc.getNodeId();
		String ip = null;
		int port = -1;
		//
		if ( !cc.getContext().isEmpty() ) {
			String[] hostAndPort = cc.getContext().toStringUtf8().split(":");
			if ( hostAndPort.length == 2 ) {
				ip = hostAndPort[0];
				port = Integer.parseInt(hostAndPort[1]);
			}
		}
		//
		switch( cc.getChangeType() ) {
		case AddNode:
		case AddLearnerNode:
			//
			if (ip != null && port != -1 && !peerSet.isExist(id, ip, port)) {
				boolean isLearner = (cc.getChangeType() == ConfChangeType.AddLearnerNode);
				this.peerSet.put(new Peer(id, ip, port, isLearner));
				isChanged = true;
			}
			break;
		case RemoveNode :
			// 待移除的节点不能是自己
			if (id != raft.getId()) {
				Peer peer = peerSet.remove(id);
				if (peer != null) {
					isChanged = true;
				} else {
					LOGGER.info("self={}, node={} not found", raft.getId(), id);
				}
			} else {
				LOGGER.info("self={} can't remove myself node", raft.getId());
			}
			break;
		default:
			break;
		}
		return isChanged;
	}

	//
	// receive snapshot from remote raft and write it to local storage
	public void saveSnapshotOfRemote(Snapshot snapshot) throws RaftException {
		AbstractSnapshotter.Writeable writer = snapshotter.create(true);
		writer.write(snapshot);
	}

	//
	// local snapshot
    protected void maybeTriggerLocalSnapshot() throws RaftException {
    	//
        // 本地快照与发送快照需要互斥
        if ( snapshotSendingCAS.hasLock() )
        	return;
        final long appliedIndex = raft.getRaftLog().getApplied();
        final long snapshotIndex = wal.getStart().getIndex();
        final long snapCount = raftCfg.getCc().getSnapCount();
        if( appliedIndex - snapshotIndex <= snapCount )
            return;
        //
        //  密集写入可能造成的频繁创建 snapshot ， 此处通过 snapshot interval 进行优化
        final long now = TimeUtil.currentTimeMillis();
        long interval = now - lastSnapshotTime;
        if ( interval < raftCfg.getCc().getSnapInterval() )
        	return;
        //
        // 是否正在创建本地快照
        if ( !isCreatingSnapshot.compareAndSet(false, true) )
        	return;
        //
        try {
        	//
        	this.notifyTpExecutor.execute( new Runnable() {
				@Override
    			public void run() {
    				try {
			            //
			            lastSnapshotTime = now;
						//
						final ConfState confState = ConfState.newBuilder() //
								.addAllNodes( raft.getPrs().voterNodes() ) //
								.addAllLearners( raft.getPrs().learnerNodes() ) //
								.build(); //

						LOGGER.info("#maybeTriggerLocalSnapshot, cs={}, appliedIndex={}, snapshotIndex={}, snapCount={}",
								confState != null ? ProtobufUtils.protoToJson(confState) : "", appliedIndex, snapshotIndex, snapCount);
						//
                        // 本地快照, 由状态机返回应用的 snapshot 数据
						if ( isDeltaSnapshot ) {
							//
							stateMachine.takeSnapshot(new StateMachine.SnapshotHandler( isDeltaSnapshot ) {
								//
								DeltaSnapshotter deltaSnapshotter = (DeltaSnapshotter)snapshotter;
								
								@Override
								public boolean handleDeltaNegotiation(List<Newdbpb.ColumnFamilyHandle> allCfh) 
										throws RaftException {
									return deltaSnapshotter.handleDeltaNegotiation(allCfh);
								}
								
								@Override
								public void handleDelta(Newdbpb.ColumnFamilyHandle cfh, byte[] data, long seqNo, boolean last) 
										throws RaftException {
									//
									Snapshot localSnap = storage.createSnapshot(appliedIndex, confState, data, seqNo, last);
									if (localSnap != null) {
										deltaSnapshotter.handleDelta(cfh, localSnap); ////
									}
								}

								@Override
								public void handleDeltaEnd() throws RaftException {
									//
									long term = storage.getTerm(appliedIndex);
									deltaSnapshotter.handleDeltaEnd(appliedIndex, term, confState);
									//
									wal.saveSnapMeta(appliedIndex, term);    //
								}
								
								@Override
								public void handle(byte[] data, long seqNo, boolean last) throws RaftException {
									throw new RaftException("Delta snapshots please.");
								}
							});
							
						} else {
							//
							stateMachine.takeSnapshot(new StateMachine.SnapshotHandler() {
								AbstractSnapshotter.Writeable sanpWriteable = snapshotter.create(false);
								@Override
								public void handle(byte[] data, long seqNo, boolean last) throws RaftException {
									Snapshot localSnap = storage.createSnapshot(appliedIndex, confState, data, seqNo, last);
									if (localSnap != null) {
										sanpWriteable.write(localSnap); //
										if (last) //
											wal.saveSnapMeta(localSnap.getMetadata());    //
									}
								}
							});
						}
						//
						// ------------------------------------------------------
						//
						// 快照之前的 Write Ahead log 的清理
						long compactIndex = 1L;
						if (appliedIndex > snapCount)
							compactIndex = appliedIndex - snapCount;
						//
						raft.getRaftLog().getStorage().compact(compactIndex);
						LOGGER.info("storage compacted log at compactIndex={},  appliedIndex={} ", compactIndex, appliedIndex);

    				} catch (RaftException e) {
    					LOGGER.error("local snapshot err:", e);

    				} finally {
    					isCreatingSnapshot.set(false);
    				}
    			}
        	});

    	} catch (RejectedExecutionException e) {
			LOGGER.error("local snapshot reject execution err:", e);
			isCreatingSnapshot.set(false);
		}
    }

	@Override
	public void onStateChange(long id, StateType newStateType, final long leaderId) {
		if ( leaderId == Const.None  )
			return;

		// raft 状态变更通知状态机
		if ( this.leaderId != Const.None && this.leaderId == leaderId && this.leaderId == id )
			return;

		LOGGER.info("onStateChange, id={}, newStateType={}, leaderId={}", id, newStateType, leaderId);
		this.leaderId = leaderId;
		//
		try {
			this.notifyTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					stateMachine.leaderChange( leaderId );
				}
			});
		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: id={}, newStateType={}, leaderId={}  ex={} ",
					id, newStateType, leaderId, ExceptionUtils.getStackTrace(e));
		}
	}

	@Override
	public void onReadIndex(final String rctx, final long index) {
		//
		// Asynchronous Lease Read 优化
		final long appliedIndex = raft.getRaftLog().getApplied();
		try {
			this.notifyTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					callbackRegistry.notifyCallbacks(rctx, index, appliedIndex);
				}
			});
		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: rctx={}, index={},  ex={} ",
					rctx, index, ExceptionUtils.getStackTrace(e));
		}
	}

	@Override
	public void onAppliedIndex(final long appliedIndex) {
		// Asynchronous
		try {
			this.notifyTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					callbackRegistry.notifyCallbacks( appliedIndex );
				}
			});
		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: applied={}, ex={} ", appliedIndex, ExceptionUtils.getStackTrace(e));
		}
	}

	@Override
	public void onReceivedHeartbeat(Message msg) {
		//
		try {
			final long commit = msg.getLcommit();
			this.notifyTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					stateMachine.leaderCommitted( commit );
				}
			});
		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: from={}, type={}, size={}, ex={} ",
					msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
		}
	}

	@Override
	public void onProposalForwarding(Message msg) {
		try {
			//
			// Forwarding to leader
	    	String cbKey = msg.getEntries( msg.getEntriesCount() - 1 ).getCbkey();
	    	//
	        final Callback callback = callbackRegistry.remove( cbKey );
	        if ( callback != null )
	        	// Propose message
	    		transportClient.asyncPost(msg, new AsyncHttpResponseHandler() {
	    			@Override
	    			public void completed(HttpResponse response) {
	    				int code = response.getStatusCode();
	    				if (code == 200)
	    					callback.onCompleted();
	    				else
	    					callback.onFailed(response.getContent());
	    			}

	    			@Override
	    			public void failed(String reason) {
	    				String errMsg = String.format("onProposalForwarding err: key:%s; err:%s", callback.key, reason);
	    				LOGGER.warn(errMsg);
	    				callback.onFailed(errMsg.getBytes());
	    			}
	    		});
		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: from={}, type={}, size={}, ex={} ",
					msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
		}
	}

	//
	@Override
	public void onSendSnapshots(final Message msg) {
		//
		try {
			//
        	// 发送快照与本地构建快照需要互斥
        	if ( isCreatingSnapshot.get() ) {
        		LOGGER.warn("send snapshot err: to={}, Because we're creating local snapshots.", msg.getTo());
        		return;
        	}

			// Asynchronous send
	    	this.notifyTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					//
			    	if ( !snapshotSendingCAS.compareAndSet(msg.getTo(), false, true) )
			    		return;

			    	SnapshotMetadata snapMetadata = msg.getSnapshot().getMetadata();
			        long snapshotIndex = snapMetadata.getIndex();
			        long snapshotTerm = snapMetadata.getTerm();
			        //
			        boolean isFailure = false;
			        AbstractSnapshotter.Readable snapReadable = null;
			        try {
			        	snapReadable = snapshotter.open(false, snapshotIndex, snapshotTerm);
			            String fileName = snapReadable.getFile().getName();
			            //
			            while ( snapReadable.isValid() ) {
			            	//
			                Snapshot localSnap = snapReadable.next();
			                long seqNo = localSnap.getChunk().getSeqNo();
			                boolean last = localSnap.getChunk().getLast() ;
			                //
			                Message snapshotMsg = Message.newBuilder() //
			                        .setFrom( msg.getFrom() ) //
			                        .setTo( msg.getTo() ) //
			                        .setMsgType(MessageType.MsgSnapshot) //
			                        .setSnapshot( localSnap ) //
			                        .build(); //
			                //
			                if ( last ) {
			                	LOGGER.warn("send snapshot, {} {} {} is completed", msg.getTo(), fileName, seqNo);
			                } else {
			                	if ( seqNo ==  1 ) {
			                		LOGGER.warn("send snapshot, {} {} {} is started", msg.getTo(), fileName, seqNo);
			                	} else if ( seqNo != 0 && seqNo % 100 == 0 ) {
			                		LOGGER.warn("send snapshot, {} {} {}", msg.getTo(), fileName, seqNo);
			                	}
			                }
			                //
			                isFailure = transportClient.syncPost( snapshotMsg );
			                if ( isFailure )
			                	break;
			            }

			        } catch (RaftException e) {
			        	isFailure = true;
						LOGGER.warn("send snapshot err:", e);

					} finally {
						//
			            IOUtil.closeQuietly( snapReadable );
			            //
			        	// report SnapshotFailure to raft state machine. After raft state
						// machine knows about it, it would pause a while and retry sending
						// new snapshot message.
						if ( isFailure )
							reportUnreachable(msg.getTo());
						//
						reportSnapshot(msg.getTo(), isFailure);
						//
						snapshotSendingCAS.set(msg.getTo(), false);
			        }
				}
	    	});

		} catch (RejectedExecutionException e) {
			LOGGER.error("rejected execution err: from={}, type={}, size={}, ex={} ",
					msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
		}
	}

	//
	private void reportUnreachable(long id)  {
		try {
			Message msg = MessageUtil.reportUnreachable(id);
			raft.step(msg);
		} catch (RaftException e) {
			LOGGER.error("reportUnreachable err: ", e);
		}
	}

	//仅leader处理这类消息, 如果reject为false：表示接收快照成功，将切换该节点状态到探测状态,否则接收失败
	private void reportSnapshot(long id, boolean reject) {
		try {
			Message msg = MessageUtil.reportSnapshot(id, reject);
			raft.step(msg);
		} catch (RaftException e) {
			LOGGER.error("reportSnapshot err: ", e);
		}
	}

	//
	public void step(final HttpConnection conn, final Message msg)  {
		//
		raftStatistics.inc(msg);
		//
		// Asynchronous execution
		try {
			//
			this.dispatchTpExecutor.execute( new Runnable() {
				@Override
				public void run() {
					try {
						raft.step(msg);
					} catch (Exception e) {
						LOGGER.error("step err: " + ProtobufUtils.protoToJson( msg ), e);
						HttpUtil.sendError(conn, ExceptionUtils.getStackTrace(e));
					}
				}
			});
			//
		} catch (Throwable e) {
			HttpUtil.sendError(conn, ExceptionUtils.getStackTrace(e));
			LOGGER.error("step err: from={}, type={}, size={}, ex={} ",
					msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
		}
	}

	//
	public void local_step(final String cbKey, final Message msg) {
		// Asynchronous local step execution
		try {
			// Step advances the state machine using the given message
			if ( Util.isResponseMsg(msg.getMsgType()) )
				throw new Errors.RaftException(Errors.ErrStepLocalResponseMsg);
			//
			this.dispatchTpExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						raft.step(msg);
					} catch (Exception e) {
						callbackRegistry.notifyCallbacks(cbKey, Callback.Action.Error);
						//
						LOGGER.error("local_step err: " + ProtobufUtils.protoToJson(msg), e);
					}
				}
			});

		} catch (Throwable e) {
			callbackRegistry.notifyCallbacks(cbKey, Callback.Action.Error);
			//
			LOGGER.error("local_step err: from={}, type={}, size={}, ex={} ",
					msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
		}
	}
	
	//
	@Override
	public Peer getPeer() {
		return peerSet.get( getId() );
	}
	
	@Override
	public PeerSet getPeerSet() {
		return peerSet;
	}
	
	@Override
	public int getDecayPriorityGap() {
		return raftCfg.getCc().getDecayPriorityGap();
	}
	
	//
	public long getId() {
		return raftCfg.getLocal().getId();
	}

	public long getLeaderId() {
		return leaderId;
	}
	public ProgressSet getPrs() {
		return this.raft.getPrs();
	}

	public RaftCaller<Message> getRaftCaller() {
		return raftCaller;
	}

	public CallbackRegistry getCallbackRegistry() {
		return callbackRegistry;
	}

	// 返回Raft的统计信息
	public RaftStatistics getRaftStatistics() {
		return raftStatistics;
	}
}