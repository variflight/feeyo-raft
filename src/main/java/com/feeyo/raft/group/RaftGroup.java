package com.feeyo.raft.group;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Config;
import com.feeyo.raft.Const;
import com.feeyo.raft.Errors;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.ProgressSet;
import com.feeyo.raft.Raft;
import com.feeyo.raft.RaftLog;
import com.feeyo.raft.RaftNodeAdapter;
import com.feeyo.raft.RaftStatistics;
import com.feeyo.raft.Ready;
import com.feeyo.raft.SoftState;
import com.feeyo.raft.StateMachine;
import com.feeyo.raft.StateType;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.caller.callback.Callback;
import com.feeyo.raft.caller.callback.CallbackRegistry;
import com.feeyo.raft.group.proto.Raftgrouppb;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.group.proto.Raftgrouppb.Region;
import com.feeyo.raft.proto.Raftpb.ConfChange;
import com.feeyo.raft.proto.Raftpb.ConfChangeType;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.proto.util.MessageUtil;
import com.feeyo.raft.storage.FileStorage;
import com.feeyo.raft.storage.Storage;
import com.feeyo.raft.storage.snapshot.AbstractSnapshotter;
import com.feeyo.raft.storage.snapshot.DefaultSnapshotter;
import com.feeyo.raft.storage.wal.Wal;
import com.feeyo.raft.transport.client.AsyncHttpResponseHandler;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.feeyo.raft.util.HashCAS;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.Util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * Multi-Raft 需要解决的一些核心问题：
 *
 * 1、数据何如分片
 * 2、分片中的数据越来越大，需要分裂产生更多的分片，组成更多Raft-Group
 * 3、分片的调度，让负载在系统中更平均（分片副本的迁移，补全，Leader切换等等）
 * 4、一个节点上，所有的Raft-Group复用链接（否则Raft副本之间两两建链，链接爆炸了）
 * 5、如何处理stale的请求（例如Proposal和Apply的时候，当前的副本不是Leader、分裂了、被销毁了等等）
 * 6、Snapshot如何管理（限制Snapshot，避免带宽、CPU、IO资源被过度占用)
 *
 * @author zhuam
 *
 */
public class RaftGroup extends RaftNodeAdapter {
    //
    private static Logger LOGGER = LoggerFactory.getLogger( RaftGroup.class );
    //
    public static final int TICK_TIMEOUT = 100;

    //
    // include  region & region epoch & peers
    private Region region;

    protected String storageDir = null;
    protected Config cfg;

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
    private StateMachine stateMachineDelegate;
    //
    private final RaftStatistics raftStatistics;
    private final CallbackRegistry callbackRegistry;

    //
    // Shared links and threads between all groups
    private RaftGroupServer raftGroupSrv;

    //
    public RaftGroup(final RaftGroupServer raftGroupSrv, Region region, Config cfg) {
        //
        this.raftGroupSrv = raftGroupSrv;
        this.region = region;
        this.cfg = cfg;
        this.storageDir = cfg.getStorageDir() + File.separator + region.getId() + File.separator;
        //
        final long regionId = this.region.getId();
        //
        // State machine delegate
        this.stateMachineDelegate = new StateMachine(){
            //
            RaftGroupStateMachine target = raftGroupSrv.getStateMachine();
            //
            @Override
            public void initialize(long committed) {
                target.initialize(regionId, committed);
            }

            @Override
            public boolean apply(byte[] data, long committed) {
                return target.apply(regionId, data, committed);
            }

            @Override
            public void applyMemberChange(PeerSet peerSet, long committed) {
                target.applyMemberChange(regionId, peerSet, committed);
            }

            @Override
            public void applySnapshot(boolean toCleanup, byte[] data) {
                target.applySnapshot(regionId, toCleanup, data);
            }

            @Override
            public void takeSnapshot(SnapshotHandler handler) throws RaftException {
                target.takeSnapshot(regionId, handler);
            }

            @Override
            public void leaderChange(long leaderId) {
                target.leaderChange(regionId, leaderId);
            }

            @Override
            public void leaderCommitted(long leaderCommitted) {
                target.leaderCommitted(regionId, leaderCommitted);
            }
        };
        //
        this.raftStatistics = new RaftStatistics();
        this.callbackRegistry = new CallbackRegistry( raftStatistics );
        this.storage = new FileStorage( storageDir );
        //
		// initially set to max(priority of all nodes)
        this.targetPriority = getMaxPriorityOfNodes();
        this.electionTimeoutCounter = 0;
    }

    //
    public void start() throws Throwable {
        //
        // 1、Loading snapshot metadata
        this.snapshotter = new DefaultSnapshotter( storageDir );
        SnapshotMetadata snapshotMeta = snapshotter.getMetadata();
        LOGGER.info("self={}, replay snapshot, meta={} ", getId(), ProtobufUtils.protoToJson( snapshotMeta ) );

        // 2、Replay WAL log files
        this.wal = new Wal(storageDir, cfg.getMaxLogFileSize(), cfg.isSyncLog());
        Wal.ReadAllLogs walLogs = this.wal.readAll( snapshotMeta == null ? -1 : snapshotMeta.getIndex() );
        LOGGER.info("self={}, replay wal, hs={}, ents={}", getId(), Util.toStr(walLogs.getHs()), Util.toStr(walLogs.getEntries()));

        // 3、Rebuilding storage
        storage.applySnapshotMetadata( snapshotMeta );
        storage.setHardState( walLogs.getHs() );
        storage.append( walLogs.getEntries() );
        LOGGER.info("self={}, replay storage, firstIdx={}, lastIdx={}", getId(), storage.firstIndex(), storage.lastIndex());

        // 4、Initialize raft
        this.raft = new Raft(cfg, storage, this);
        final RaftLog raftLog = raft.getRaftLog();
        long lastIndex = raftLog.getStorage().lastIndex();
        if ( lastIndex == 0 ) {
            //
            // If the log is empty
            raft.becomeFollower(1, Const.None);
            //
            List<Entry> entries = new ArrayList<Entry>();
            //
            // add voter
            for(long pid: cfg.getVoters()) {
                Peer peer = raftGroupSrv.getPeerSet().get( pid );
                Entry entry = newConfChange(ConfChangeType.AddNode, peer);
                entries.add( entry );
                //
                raft.addVoterOrLearner(peer.getId(), peer.isLearner());
            }

            // add learner
            for(long pid: cfg.getLearners()) {
                Peer peer = raftGroupSrv.getPeerSet().get( pid );
                Entry entry = newConfChange(ConfChangeType.AddNode, peer);
                entries.add( entry );
                //
                raft.addVoterOrLearner(peer.getId(), peer.isLearner());
            }

            raftLog.append(raft.getTerm(), entries);
            raftLog.commitTo( entries.size() ); // TODO ???
        }
        //
        this.prevSs = raft.getSoftState();
        this.prevHs = (raftLog.getStorage().lastIndex() != 0 ? raft.getHardState(): HardState.getDefaultInstance() );
        this.stateMachineDelegate.initialize( raftLog.getCommitted() );
        //
        LOGGER.info("self={}, raftLog={}", getId(), raftLog);
    }

    public void stop() {
        //
        if ( this.callbackRegistry != null )
            this.callbackRegistry.shutdown();
        
        //
        if ( this.wal != null )
            this.wal.stop();
        
        //
		if ( this.storage != null )
			this.storage.close();
    }

    //
    // 需要考虑资源永久性释放问题
    public void destory() {
        stop();
    }

    //
    // ------------------------------------ passive ------------------------------------------
    //
    private AtomicBoolean isPassiveLoop = new AtomicBoolean( false );
    private long startMs;
    //
    void passiveLoop() throws Throwable {
        //
        if ( !isPassiveLoop.compareAndSet(false, true) )
            return;
        //
        try {
            //
            // internal logical clock by a single tick
            long currentMs = TimeUtil.currentTimeMillis();
            if (currentMs - startMs > TICK_TIMEOUT) {
                startMs = currentMs;
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
            }
            //
        } finally {
            isPassiveLoop.set(false);
        }
    }

    //
    private void onNewReady(final Ready ready) throws RaftException {
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
        if ( isLeader ) {
            //
            // Batch and Pipeline
            if ( ready.messages != null && !ready.messages.isEmpty()) {
                //
                for (final List<Message> msgs : ready.messages.values()) {
                    //
                    this.raftGroupSrv.getDispatchTpExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            //
                            long bcastBeginMillis = TimeUtil.currentTimeMillis();
                            try {
                                bcastMessages(msgs);
                            } catch (Throwable e) {
                                LOGGER.error("broadcast msg err:", e);
                            } finally {
                                //
                                long elapsed = TimeUtil.since(bcastBeginMillis);
                                if ( elapsed > 50 && !Util.isNotEmpty( msgs ) ) {
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
            this.raftGroupSrv.getDispatchTpExecutor().execute( new Runnable(){
                @Override
                public void run() {
                    //
                    long tmpBeginMillis = TimeUtil.currentTimeMillis();
                    //
                    try {
                        // Write hardState & entries to log
                        wal.save( ready.unstableEntries, ready.hs );
                        raftStatistics.update(RaftStatistics.APPEND_LOGS, TimeUtil.since(tmpBeginMillis));
                        //
                        tmpBeginMillis = TimeUtil.currentTimeMillis();

                        // Write entries to local raftStorage
                        raft.getRaftLog().getStorage().append( ready.unstableEntries );
                        raft.getRaftLog().getStorage().setHardState( ready.hs );
                        //
                        raftStatistics.update(RaftStatistics.APPEND_STORAGE, TimeUtil.since(tmpBeginMillis));

                    } catch (Throwable e) {
                        LOGGER.error("append log parallelly err:", e);
                    } finally {
                        //
                        latch.countDown();
                    }
                }
            });


        } else {
            //
            // Follower Parallelly
            this.raftGroupSrv.getDispatchTpExecutor().execute( new Runnable(){
                @Override
                public void run() {
                    try {
                        //
                        // TODO: 注意事项
                        // Apply snapshot 这个地方会造成阻塞，很大可能会触发新的选举, 后续优化
                        final SnapshotMetadata snapshotMeta = ready.unstableSnapshotMeta;
                        if ( snapshotMeta != null && snapshotMeta.getIndex() > 0 ) {

                            long tmpBeginMillis = TimeUtil.currentTimeMillis();
                            //
                            long snapshotIndex = snapshotMeta.getIndex();
                            long appliedIndex = raft.getRaftLog().getApplied();
                            if( snapshotIndex <= appliedIndex  )
                                throw new Errors.RaftException("snapshot index ["+ snapshotIndex
                                        + "] should > progress.appliedIndex ["+ appliedIndex +"] + 1)");

                            // Apply snapshot to state machine
                            applySnapshotToStateMachine( snapshotMeta );
                            //
                            LOGGER.info("apply snapshot at index {}, old appliedIndex={}", snapshotIndex, appliedIndex);
                            //
                            raft.getRaftLog().stableSnapTo( snapshotIndex );
                            raft.getRaftLog().appliedTo( snapshotIndex );

                            //
                            raftStatistics.update( RaftStatistics.APPLY_SNAPSHOT, TimeUtil.since( tmpBeginMillis ) );
                        }

                        // ---------------------------------------------------------------------
                        // Write hardState & unstable entries to log
                        //
                        long tmpBeginMillis = TimeUtil.currentTimeMillis();
                        wal.save( ready.unstableEntries, ready.hs );
                        raftStatistics.update(RaftStatistics.APPEND_LOGS, TimeUtil.since(tmpBeginMillis));
                        //
                        //
                        // Write unstable entries to local raftStorage
                        tmpBeginMillis = TimeUtil.currentTimeMillis();
                        raft.getRaftLog().getStorage().append( ready.unstableEntries );
                        raft.getRaftLog().getStorage().setHardState( ready.hs );
                        raftStatistics.update(RaftStatistics.APPEND_STORAGE, TimeUtil.since(tmpBeginMillis));

                        // ---------------------------------------------------------------------
                        // broadcast message
                        if ( ready.messages != null && !ready.messages.isEmpty() ) {
                            //
                            tmpBeginMillis = TimeUtil.currentTimeMillis();
                            for(List<Message> msgs : ready.messages.values() )
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

        // Asynchronous Apply
        //
        this.raftGroupSrv.getDispatchTpExecutor().execute( new Runnable(){
            @Override
            public void run() {
                //
                try {
                    List<Entry> committedEntries = ready.committedEntries;
                    if ( Util.isEmpty( committedEntries ) )
                        return;
                    //
                    long applyBeginMillis = TimeUtil.currentTimeMillis();
                    //
                    List<Entry> applyEntries = entriesToApply( committedEntries );
                    applyEntriesToStateMachine( applyEntries );
                    //
                    raftStatistics.gauge( raft.getRaftLog().getCommitted(), raft.getRaftLog().getApplied() );
                    raftStatistics.update( RaftStatistics.APPLY_ENTRIES, TimeUtil.since( applyBeginMillis ) );

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
        //
        // NewReady Statistics
        raftStatistics.update( RaftStatistics.ON_READY, TimeUtil.since(beginMillis) );

        // Maybe trigger local snapshot
        this.maybeTriggerLocalSnapshot();

        // Commit ready
        this.commitReady(ready);
    }

    //
    // receive snapshot from remote raft and write it to local storage
    public void saveSnapshotOfRemote(Snapshot snapshot) throws RaftException {
        AbstractSnapshotter.Writeable writer = snapshotter.create(true);
        writer.write(snapshot);
    }

    //
    // 触发 local snapshot
    protected void maybeTriggerLocalSnapshot() throws RaftException {
        //
        // 本地快照与发送快照需要互斥
        if ( snapshotSendingCAS.hasLock() )
            return;

        final long appliedIndex = raft.getRaftLog().getApplied();
        final long snapshotIndex = wal.getStart().getIndex();
        final long snapCount = cfg.getSnapCount();
        if( appliedIndex - snapshotIndex <= snapCount )
            return;
        //
        //  密集写入可能造成的频繁创建 snapshot ， 此处通过 snapshot interval 进行优化
        final long now = TimeUtil.currentTimeMillis();
        long interval = now - lastSnapshotTime;
        if ( interval < cfg.getSnapInterval() )
            return;

        // 是否正在创建本地快照
        if ( !isCreatingSnapshot.compareAndSet(false, true) )
            return;
        //
        try {
            //
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
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
                        stateMachineDelegate.takeSnapshot(new StateMachine.SnapshotHandler() {
                            //
                            AbstractSnapshotter.Writeable sanpWriteable = snapshotter.create(false);
                            //
                            @Override
                            public void handle(byte[] data, long seqNo, boolean last) throws RaftException {
                                //
                                Snapshot localSnap = storage.createSnapshot(appliedIndex, confState, data, seqNo, last);
                                if ( localSnap != null ) {
                                    sanpWriteable.write( localSnap ); //
                                    if ( last ) //
                                        wal.saveSnapMeta( localSnap.getMetadata() );	//
                                }
                            }
                        });
                        //
                        // 快照之前的 write ahead log 的清理
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

    //
    //
    private void bcastMessages(List<Message> messages) {
        if ( Util.isEmpty( messages ) )
            return;

        // metric counter
        for(Message msg: messages)
            raftStatistics.inc(msg);

        // pipeline && batch
        this.raftGroupSrv.getTransportClient().pipeliningBatchPost( toGroupMessage(messages) );
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
    }

    //
    public void step(final HttpConnection conn, final Message msg)  {
        //
        raftStatistics.inc( msg );
        //
        // Asynchronous execution
        try {
            //
            this.raftGroupSrv.getDispatchTpExecutor().execute( new Runnable() {
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
            this.raftGroupSrv.getDispatchTpExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        raft.step(msg);
                    } catch (Exception e) {
                        getCallbackRegistry().notifyCallbacks(cbKey, Callback.Action.Error);
                        //
                        LOGGER.error("local_step err: " + ProtobufUtils.protoToJson(msg), e);
                    }
                }
            });

        } catch (Throwable e) {
            getCallbackRegistry().notifyCallbacks(cbKey, Callback.Action.Error);
            //
            LOGGER.error("local_step err: from={}, type={}, size={}, ex={} ",
                    msg.getFrom(), msg.getMsgType(), msg.getSerializedSize(), ExceptionUtils.getStackTrace(e));
        }
    }

    //
    // -------------------------------------------------------------------------------------------------------
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
                    //
                	long idx = ent.getIndex();
	            	byte[] data = ZeroByteStringHelper.getByteArray( ent.getData() );
	            	//
                    boolean isOk = stateMachineDelegate.apply(data, idx);
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
//							if ( transportClient != null )
//								transportClient.reload( peerSet );
                            //
                            // 通知Raft内部 progress 信息改变
                            raft.applyConfChange(cc);
                            //
                            // 通知状态机，成员改变
                            this.stateMachineDelegate.applyMemberChange(raftGroupSrv.getPeerSet(), ent.getIndex());
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
    private Entry newConfChange(ConfChangeType type, Peer peer) {
        //
        ConfChange cc = ConfChange.newBuilder()
                .setChangeType(type)	//
                .setNodeId(peer.getId())	//
                .setContext(ByteString.copyFromUtf8(peer.getEndpoint().toString())) //
                .build(); //
        Entry entry = Entry.newBuilder()	//
                .setEntryType(EntryType.EntryConfChange)	//
                .setData(ZeroByteStringHelper.wrap(cc.toByteArray())).build(); //
        return entry;
    }

    //
    private boolean applyConfChange(ConfChange cc) {
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
                if (ip != null && port != -1 && !raftGroupSrv.getPeerSet().isExist(id, ip, port)) {
                    boolean isLearner = (cc.getChangeType() == ConfChangeType.AddLearnerNode);
                    this.raftGroupSrv.getPeerSet().put(new Peer(id, ip, port, isLearner));
                    isChanged = true;
                }
                break;
            case RemoveNode :
                // 待移除的节点不能是自己
                if (id != raft.getId()) {
                    Peer peer = raftGroupSrv.getPeerSet().remove(id);
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
                    stateMachineDelegate.applySnapshot( seqNo == 1, ZeroByteStringHelper.getByteArray( data ) );  // 确定业务状态机需要做历史数据的 cleanup
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
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
                @Override
                public void run() {
                    stateMachineDelegate.leaderChange( leaderId );
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
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
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
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
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
        try {
            final long commit = msg.getLcommit();
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
                @Override
                public void run() {
                    stateMachineDelegate.leaderCommitted( commit );
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
            // Forwarding to leader
            String cbKey = msg.getEntries( msg.getEntriesCount() - 1 ).getCbkey();
            //
            final Callback callback = callbackRegistry.remove( cbKey );
            if ( callback != null )
                // Propose message
                raftGroupSrv.getTransportClient().asyncPost(toGroupMessage(msg), new AsyncHttpResponseHandler() {
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

    @Override
    public void onSendSnapshots(final Message msg) {
        try {
            // 发送快照与本地构建快照需要互斥
            if ( isCreatingSnapshot.get() ) {
                LOGGER.warn("send snapshot err: to={}, Because we're creating local snapshots.", msg.getTo());
                return;
            }

            // Asynchronous send
            this.raftGroupSrv.getNotifyTpExecutor().execute( new Runnable() {
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
                            isFailure = raftGroupSrv.getTransportClient().syncPost( toGroupMessage(snapshotMsg) );
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

    //
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
    // TODO: 此种实现待优化，应该放到Ready 中构造  RaftGroupMessage
    private RaftGroupMessage toGroupMessage(Message message) {
        //
        Raftgrouppb.Peer toPeer = Raftgrouppb.Peer.newBuilder() //
                .setId( message.getTo() ) //
                .build();
        //
        Raftgrouppb.Peer fromPeer = Raftgrouppb.Peer.newBuilder() //
                .setId( message.getFrom() ) //
                .build();
        //
        return RaftGroupMessage.newBuilder() //
                .setRegionId( region.getId() ) //
                .setStartKey( region.getStartKey() ) //
                .setEndKey( region.getEndKey() ) //
                .setRegionEpoch( region.getRegionEpoch() ) //
                .setIsTombstone( false ) //
                .setToPeer( toPeer ) //
                .setFromPeer( fromPeer ) //
                .setMessage(message) //
                .build(); 
    }

    private List<RaftGroupMessage> toGroupMessage(List<Message> messages) {
        List<RaftGroupMessage> gMessages = new ArrayList<>( messages.size() );
        for(Message msg: messages)
            gMessages.add( toGroupMessage(msg) );
        return gMessages;
    }
    
    ///
	@Override
	public Peer getPeer() {
		return getPeerSet().get( getId() );
	}
	
    @Override
	public PeerSet getPeerSet() {
		return this.raftGroupSrv.getPeerSet();
	}

	///

    public long getId() {
        return cfg.getId();
    }

    public long getRegionId() {
        return region.getId();
    }

    public long getLeaderId() {
        return leaderId;
    }

    public ProgressSet getPrs() {
        return this.raft.getPrs();
    }

    public CallbackRegistry getCallbackRegistry() {
        return callbackRegistry;
    }

    RaftStatistics getMetrics() {
        return raftStatistics;
    }

    AbstractSnapshotter getSnapshotter() {
        return snapshotter;
    }

}