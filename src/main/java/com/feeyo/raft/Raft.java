package com.feeyo.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.Progress.Inflights;
import com.feeyo.raft.Progress.ProgressState;
import com.feeyo.raft.proto.Raftpb.ConfChange;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.storage.Storage;
import com.feeyo.raft.util.Util;
import com.feeyo.raft.util.HashCAS;
import com.feeyo.raft.util.Pair;
import com.google.protobuf.ZeroByteStringHelper;

/**
 * @see https://github.com/coreos/etcd/blob/master/raft/raft.go
 *  
 * @author xuwenfeng
 * @author zhuam
 */
public class Raft {
	
	private static Logger LOGGER = LoggerFactory.getLogger( Raft.class );

	volatile long id;	// 节点的 id
	volatile long term;	// 任期号
	volatile long vote; // 投票给哪个节点id
	
	boolean isLearner;
	
	Map<Long, Boolean> votes;	// 该map存放哪些节点投票给了本节点
	
	 // the log
	RaftLog raftLog;
	
	private int maxInflight;
	private long maxMsgSize;
	
	ProgressSet prs;
	
	volatile StateType state;
	
	private ConcurrentLinkedQueue<Message> msgs;
	
	// the leader id
	volatile long leaderId;
	
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	volatile long leadTransferee;	// leader转让的目标节点id
	
	// Only one conf change may be pending (in the log, but not yet applied) at a time. This is enforced via pendingConfIndex, 
	// which is set to a value >= the log index of the latest pending configuration change (if any). 
	// Config changes are only allowed to be proposed if the leader's applied index is greater than this value
	volatile long pendingConfIndex; // 标识当前还有没有applied的配置
	
	ReadOnly readOnly;
	
	/// number of ticks since it reached last electionTimeout when it is leader or candidate.
    /// number of ticks since it reached last electionTimeout or received a valid message from current leader when it is a follower.
	volatile int electionElapsed;
	
	/// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
	volatile int heartbeatElapsed;
	
	// 标记leader是否需要检查集群中超过半数节点的活跃性，如果在选举超时内没有满足该条件，leader切换到follower状态
	boolean checkQuorum;
	private boolean preVote;
	
	// This may make follower wait a longer time to apply an entry. This configuration
	// May affect proposal forwarding and follower read.
	boolean skipBcastCommit;
	
	volatile int heartbeatTimeout;
	volatile int electionTimeout;
	
	// randomized_election_timeout is a random number between
    // [min_election_timeout, max_election_timeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
	volatile int randomizedElectionTimeout;
	
	private int minElectionTimeout;
	private int maxElectionTimeout;
	
	boolean disableProposalForwarding;
	//
	private Object _requestVoteLock = new Object();
	
	// 
	RaftListener listener; 
	
	//
	// 逆序排列
	private Comparator<Long> reverseComparator = new Comparator<Long>() {
		@Override
		public int compare(Long o1, Long o2) {
			if(o2 == null)
				return -1;
			return o2.compareTo(o1);
		}
	};
	
	public Raft(Config cfg, Storage storage, RaftListener listener) throws RaftException {
		//
		// raft configure validate
		cfg.validate();
		
		// MaxCommittedSizePerReady
		this.raftLog = new RaftLog(listener, storage, cfg.getMaxSizePerMsg());		
		this.listener = listener;

		//
		List<Long> voters = cfg.getVoters();
		List<Long> learners = cfg.getLearners();
		
		// 从 Storage 中恢复 raft 状态
		Pair<HardState, ConfState> pair = storage.initialState();
		HardState hs = pair.first;
		ConfState cs = pair.second;
		LOGGER.info("raft initial state, hs={}, cs={}", Util.toStr(hs), Util.toStr(cs));
		//
		// 合并 configuration 和 confState 中的 peers, 以 configuration 为准
		if ( cs != null ) {
			if ( cs.getLearnersList() != null ) 
				for (long id : cs.getLearnersList()) 
					if (!(voters.contains(id) || learners.contains(id))) 
						learners.add(id);
	
			if ( cs.getNodesList()  != null ) 
				for (long id : cs.getNodesList()) 
					if (!(voters.contains(id) || learners.contains(id))) 
						voters.add(id);
		} 
		
		//
		this.id = cfg.getId();
		this.msgs = new ConcurrentLinkedQueue<Message>();
		this.maxMsgSize = cfg.getMaxSizePerMsg();
		this.maxInflight = cfg.getMaxInflightMsgs();
		this.prs = new ProgressSet(voters.size(), learners.size());
		this.state = StateType.Follower;
		this.isLearner = false;
		this.checkQuorum = cfg.isCheckQuorum();
		this.preVote = cfg.isPreVote();
		this.readOnly = new ReadOnly(cfg.getReadOnlyOption());
		this.heartbeatTimeout = cfg.getHeartbeatTick();
		this.electionTimeout = cfg.getElectionTick();
		this.votes = new ConcurrentHashMap<Long, Boolean>();
		
		this.leaderId = Const.None;
		this.leadTransferee = Const.None;
		this.term = Const.ZERO_TERM;
		this.electionElapsed = 0;
		this.pendingConfIndex = 0;
		this.vote = Const.None;
		this.heartbeatElapsed = 0;
		this.randomizedElectionTimeout = 0;
		this.minElectionTimeout = cfg.minElectionTick();
		this.maxElectionTimeout = cfg.maxElectionTick();
		this.skipBcastCommit = cfg.isSkipBcastCommit();
		this.disableProposalForwarding = cfg.isDisableProposalForwarding();
		
		// 
		for (long p : voters) {
			Progress pr = new Progress(1, new Inflights(maxInflight));
			this.prs.insertVoter(p, pr);
		}

		for (long p : learners) {
			Progress pr = new Progress(1, new Inflights(maxInflight));
			pr.setLearner(true);
			this.prs.insertLearner(p, pr);

			if (p == this.id) {
				this.isLearner = true;
			}
		}

		// load HardState
		if (hs != null && hs != HardState.getDefaultInstance()) 
			loadState(hs);
		//
		if (cfg.getApplied() > 0) 
			this.raftLog.setApplied(cfg.getApplied());
		//
		String info = String.format("self=%d, init raft, [last logterm: %d, last index: %d, commit: %d, applied: %d]", 
				this.id, raftLog.lastTerm(), raftLog.lastIndex(), raftLog.getCommitted(), raftLog.getApplied());
		LOGGER.info(info);
		
		// 启动都是follower状态
		this.becomeFollower(term, Const.None);	

	}
	
	//
	private void loadState(HardState state) throws RaftException {
		//
		// 已经 commit 的 log index
		long commit = state.getCommit();
		if( commit < raftLog.getCommitted() || commit > raftLog.lastIndex()) {

			StringBuffer errorMsg = new StringBuffer();
			errorMsg.append("self=").append(this.id);
			errorMsg.append(" hard state commit ").append( commit );
			errorMsg.append(" is out of range [committed: ").append(raftLog.getCommitted());
			errorMsg.append(", last index: ").append(raftLog.lastIndex()).append("]").toString();
			
			throw new Errors.RaftException( errorMsg.toString() );
		}
		
		// TODO: 此处需要 fix
		long applied = state.getApplied();
		if ( applied < raftLog.firstIndex() )
			applied = commit;
		
		this.raftLog.setCommitted( commit );
		this.raftLog.setApplied( applied  );
		this.term = state.getTerm();
		this.vote = state.getVote();
	}
	
	public int getPendingReadCount() {
		return readOnly.pendingReadCount();
	}
	
	public SoftState getSoftState() {
		return new SoftState(leaderId, state);
	}
	
	public boolean isChangedSoftState(SoftState prevSs) {
		if ( prevSs != null && ( prevSs.getId() != leaderId || prevSs.getState() != state )) {
			return true;
		}
		return false;
	}
	
	public HardState getHardState() {
		
		return HardState.newBuilder() //
				.setTerm(term) //
				.setVote(vote) //
				.setCommit(raftLog.getCommitted()) //
				.setApplied(raftLog.getApplied())
				.build(); //
	}
	
	public boolean isChangedHardState(HardState prevHs) {
		
		if ( prevHs.getTerm() != term || 
				prevHs.getVote() != vote ||  
				prevHs.getCommit() != raftLog.getCommitted() ||
				prevHs.getApplied() != raftLog.getApplied()) {
			return true;
		}
		
		return false;
	}

	public boolean inLease() {
		return this.state == StateType.Leader && this.checkQuorum;
	}
	
	//
	public boolean isLeader() {
		return this.state == StateType.Leader;
	}
	
	//
	public boolean isFollower() {
		return this.state == StateType.Follower;
	}
	
	// 法定人数 & 超过半数的节点数量
	int quorum(int total) {
	    return (total / 2) + 1;
	}

	// 法定人数
	int quorum() {
		int total = this.prs.voters().size();
		return quorum( total );
	}

	// send persists state to stable storage and then sends to its mailbox.
	void send(Message msg) throws RaftException {

		Message.Builder builder = msg.toBuilder();
		
		// TODO: fixed 
		//  Because step_follower forward MsgTransferLeader Msg to leader，but step_leader use msg.from to leadTransferee
		//
		if ( msg.getMsgType() != MessageType.MsgTransferLeader)
			builder.setFrom( id );  //

		if (msg.getMsgType() == MessageType.MsgRequestVote 
				|| msg.getMsgType() == MessageType.MsgRequestPreVote
				|| msg.getMsgType() == MessageType.MsgRequestVoteResponse
				|| msg.getMsgType() == MessageType.MsgRequestPreVoteResponse) {
			
			// 投票时term不能为空
			if (msg.getTerm() == 0) {
				// All {pre-,}campaign messages need to have the term set when
				// sending.
				// - MsgVote: m.Term is the term the node is campaigning for, non-zero as we increment the term when campaigning.
				// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was granted, non-zero for the same reason MsgVote is
				// - MsgPreVote: m.Term is the term the node will campaign, non-zero as we use m.Term to indicate the next term we'll be campaigning for
				// - MsgPreVoteResp: m.Term is the term received in the original
				//   MsgPreVote if the pre-vote was granted, non-zero for the same reasons MsgPreVote is
				 throw new Errors.RaftException("term should be set when sending " + msg.getMsgType() );
			}
			
		} else {
			
			// 其他的消息类型，term必须为空
			if (msg.getTerm() != 0) {	
				throw new Errors.RaftException("term should not be set when sending "+ msg.getMsgType() + " (was " + msg.getTerm() + ")");
			}

			// do not attach term to MsgProp, MsgReadIndex 
            // proposals are a way to forward to the leader and should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
			//
			// MsgPropose消息 和 MsgReadIndex消息 不需要带上term参数
			if (msg.getMsgType() != MessageType.MsgPropose && msg.getMsgType() != MessageType.MsgReadIndex) {
				builder.setTerm(this.term);
			}
			
			// 
			if ( msg.getMsgType() == MessageType.MsgPropose ) {
				listener.onProposalForwarding(msg);
				return;
			}
		}
		
		// TODO： 注意事项
		// 这里只是添加到 msgs 中， 但消息并没有在这里被发送出去
		Message newMsg = builder.build();
		this.msgs.add( newMsg );
	}
	
    // 向to节点发送append消息
    void sendAppend(long to) throws RaftException {
    	maybeSendAppend(to, true); // true
	}
    
    // HashLock
    
    //  
    // private HashLock hashLock = new HashLock();
    
    private HashCAS hashCas = new HashCAS();
    
	 // maybeSendAppend sends an append RPC with new entries to the given peer,
	 // if necessary. Returns true if a message was sent. The sendIfEmpty
	 // argument controls whether messages with no entries will be sent
	 // ("empty" messages are useful to convey updated Commit indexes, but
	 // are undesirable when we're sending multiple messages in a batch).
    boolean maybeSendAppend(long to, boolean sendIfEmpty) throws RaftException {
		//
    	// has CAS 替代 hash Lock
    	if ( !hashCas.compareAndSet(to, false, true) ) 
    		return false;
    	//
		try {
			//
			Progress pr = prs.get(to);
			if ( pr.isPaused() ) 
				return false;
		
			Message.Builder msgBuilder = Message.newBuilder();
			msgBuilder.setTo(to);
			try {
				// 从该节点的Next的上一条数据获取term
				long nextIdx = pr.getNextIdx();
				long term = this.raftLog.term(nextIdx - 1);
				
				// 获取从该节点的Next之后的entries，总和不超过maxMsgSize
				List<Entry> ents = this.raftLog.entries(nextIdx, this.maxMsgSize);

				// @see https://github.com/etcd-io/etcd/blob/master/raft/raft.go 
				// 
				if( term == Const.ZERO_TERM  ) {
					String errMsg = String.format("we failed to get term, because it is zero. sendIfEmpty=%s, raftLog=%s,  to=%s, pr=%s", 
							sendIfEmpty, this.raftLog.toString(),  to, pr.toString());
					throw new Errors.RaftException(errMsg);
				}

				// follower is the same as leader
				if( Util.isEmpty( ents ) && !sendIfEmpty ) 
					return false;
				
				// 
				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("self={}, maybe send append to={}  nextIdx={}, term={}, ents={}, maxMsgSize={}", 
						this.id, to, nextIdx, term,  Util.toStr(ents), maxMsgSize);

				// prepare send entries
				msgBuilder.setMsgType(MessageType.MsgAppend);
				msgBuilder.setIndex(nextIdx - 1);
				msgBuilder.setLogTerm(term);
				msgBuilder.addAllEntries(ents);
				
				// append消息需要告知当前leader的commit索引
				msgBuilder.setCommit(this.raftLog.getCommitted());	
				
				// 如果发送过去的entries不为空
				int n = ents.size();
				if (n != 0) {
					// optimistically increase the next when in ProgressState.Replicate
					//
					final ProgressState state = pr.getState();
					if ( state == ProgressState.Replicate) {
						// 如果该节点在接受副本的状态， 获取待发送数据的最后一条索引
						long last = ents.get(n - 1).getIndex();
						// 直接使用该索引更新Next索引
						pr.optimisticUpdate(last);
						pr.getIns().add(last);
						
					} else if ( state == ProgressState.Probe) {
						// 在probe状态时，每次只能发送一条app消息
						pr.pause();
						
					} else {
						//
						LOGGER.warn("{} is sending append in unhandled state, from {} {}/{}", this.id ,  to, state, pr.getState() );
						//throw new Errors.RaftException(this.id + " is sending append in unhandled state " + state);
						return false;
					}
				}
				
				//
				Message msg = msgBuilder.build();
				send( msg );
				
			} catch (Errors.RaftException e1) {
				
				// send snapshot
				LOGGER.warn("self={}, need send snapshot to node={}, pr={}, raftLog={}, ex={}", 
						this.id, to, pr, raftLog.toString(), ExceptionUtils.getStackTrace(e1) );
				
				// send snapshot if we failed to get term or entries				
				// 如果该节点当前不可用
				if( !pr.isRecentActive() ) {
					LOGGER.warn("self={}, ignore sending snapshot to node={} since it is not recently active", this.id, to);
					return false;
				}

				//
				// 尝试发送快照
				msgBuilder.setMsgType(MessageType.MsgSnapshot);
				
				try {	
					// 不能发送空快照
					SnapshotMetadata snapMeta = raftLog.getSnapshotMetadata();
					if ( snapMeta == null || snapMeta.getIndex() == 0 ) 
						throw new Errors.RaftException( Errors.ErrEmptySnapshot );
					
					Snapshot snapshot = Snapshot.newBuilder().setMetadata( snapMeta ).build();
					msgBuilder.setSnapshot( snapshot );
					//
					long sindex = snapMeta.getIndex();
					long sterm = snapMeta.getTerm();
					//
					LOGGER.info("self={}, [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to node={}[{}]",
							this.id, raftLog.firstIndex(), raftLog.getCommitted(), sindex, sterm, to, pr);
					//
					// 该节点进入接收快照的状态
					pr.becomeSnapshot( sindex );
					LOGGER.info("self={}, paused sending replication messages to node={} [{}]", this.id, to, pr);
					
					//
					Message msg = msgBuilder.build();
					//send( msg );
					
					listener.onSendSnapshots(msg);

				} catch (Errors.RaftException e2) {
					
					// panic("need non-empty snapshot")
					// TODO: 此处待进一步确认
					if ( Errors.ErrEmptySnapshot.equalsIgnoreCase( e2.getMessage() ) ) {
						LOGGER.error("self={}, failed to send snapshot to node={} because snapshot is empty", this.id, to);
						return false;
					}
					
					if ( Errors.ErrSnapshotTemporarilyUnavailable.equalsIgnoreCase( e2.getMessage() ) ) {
						LOGGER.error("self={}, failed to send snapshot to {} because snapshot is temporarily \\ unavailable", this.id, to);
						return false;
					}					
					throw e2;
				}
			}	
			return true;
			
		} finally {
			//
			//hashLock.unlock( to );
			hashCas.set(to, false);
		}
    }

	// sendHeartbeat sends an empty MsgApp
	private void sendHeartbeat(long to, byte[] ctx) throws RaftException {	
		// commit index 取需要发送过去的节点的 match 和当前 leader 的 commited 中的较小值
		Progress pr = prs.get(to);
		long matched = pr.getMatched();
		long commit = Math.min(matched, raftLog.getCommitted());
		//
		Message.Builder builder = Message.newBuilder();
		builder.setTo(to);
		builder.setMsgType(MessageType.MsgHeartbeat);
		builder.setCommit(commit);
		builder.setLcommit( raftLog.getCommitted() );	// TODO: 自己扩展的
		
		if(ctx != null)
			builder.setContext(ZeroByteStringHelper.wrap(ctx));
		
		send( builder.build() );
	}

	// bcastAppend sends RPC, with entries to all peers that are not up-to-date
	// according to the progress recorded in r.prs.
	//
	// 向所有follower发送append消息
	void bcastAppend() throws RaftException {
		// forEachProgress
		for(long pid : prs.mut()) {
			if(pid != this.id ) {
				sendAppend( pid );
			}
		}
	}
	
	// bcastHeartbeat sends RPC, without entries to all the peers.
	void bcastHeartbeat() throws RaftException {
		byte[] ctx = this.readOnly.lastPendingRequestCtx();
		bcastHeartbeatWithCtx(ctx);
	}
	
	void bcastHeartbeatWithCtx(byte[] ctx) throws RaftException {
		for(long pid : prs.mut()) {
			if( pid != this.id) {
				sendHeartbeat(pid, ctx);
			}
		}
	}
	
	// maybe_commit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call r.bcast_append).
	//
	// 尝试 commit 当前的日志，如果commit日志索引发生变化了就返回true
	boolean maybeCommit() throws RaftException {
		//
		// TODO: leader commit 核心流程
		//--------------------------------------
		//
		// 拿到当前所有节点的Match到数组中
		List<Long> mis = new ArrayList<Long>(prs.mut().size());
		for(long pid: prs.mut()) {
			Progress pr = prs.get(pid);
			mis.add( pr.getMatched() );
		}
		// 逆排序
		Collections.sort(mis, reverseComparator);
		//
		// 排序之后拿到中位数的 Match，因为如果这个位置的Match对应的Term也等于当前的Term
		// 说明有过半的节点至少commit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
		long mci = mis.get(quorum() -1);
		//
		// raft日志尝试commit
		return this.raftLog.maybeCommit(mci, term);
	}

	// 重置raft的一些状态
	private void reset(long term) {
		
		if( this.term != term ) {
			// 如果是新的任期，那么保存任期号，同时将投票节点置空
			this.term = term;
			this.vote = Const.None;
		}
		
		this.leaderId = Const.None;
		this.electionElapsed = 0;
		this.heartbeatElapsed = 0;
		
		// 重置选举超时
		this.resetRandomizedElectionTimeout();
		//
		this.abortLeaderTransfer();
		
		this.votes.clear();
		this.pendingConfIndex = 0;
		this.readOnly = new ReadOnly(readOnly.option); 
		
		//
		long lastIndex = this.raftLog.lastIndex();
		for(long pid: prs.mut()) {
			Progress p = prs.get(pid);
			p.setNextIdx(lastIndex + 1);
			if (this.id == pid ) 
				p.setMatched(lastIndex);
		}
	}
	
	private Object _lock = new Object();
	
	// 批量append一堆entries
	boolean appendEntry(List<Entry> es) throws RaftException {
		
		// use latest "last" index after truncate/append
		long lastIndex = this.raftLog.append(this.term, es);
		
		//
		synchronized ( _lock ) {
			// 更新本节点的Next以及Match索引
			this.prs.get( this.id ).maybeUpdate(lastIndex);	
			// append之后，尝试一下是否可以进行commit
			this.maybeCommit();
		}
		return true;
	}
	
	//
	// Raft 心跳信号，有两个很重要的超时机制：心跳保持和Leader选举, 周期性的调用 tick() 函数，以便为超时机制服务
	public void tick() throws RaftException {
		//
		// tick函数，在到期的时候调用，不同的角色该函数不同
		switch( state ) {
		case Follower:
		case PreCandidate:
		case Candidate:
			TickElection.tick(this);
			break;
		case Leader:
			TickHeartbeat.tick(this);
		}
	}
	
	//
	public void becomeFollower(long term, long leaderId) {
		//
		this.reset(term);
		this.leaderId = leaderId;
		this.state = StateType.Follower;
		this.listener.onStateChange( id, StateType.Follower, leaderId );
		LOGGER.info("self={}, became follower at term {} leaderId={}", this.id, term, leaderId);
	}
	
	private void becomeCandidate() throws RaftException {
		//
		// TODO(xiangli) remove the panic when the raft implementation is stable
		if(this.state == StateType.Leader) 
			throw new Errors.RaftException("invalid transition [leader -> candidate]");

		//
		// 因为进入candidate状态，意味着需要重新进行选举了，所以reset的时候传入的是Term+1
		this.reset( this.term + 1 );	// next term
		
		// 给自己投票
		this.vote = id;
		this.state = StateType.Candidate;
		this.listener.onStateChange( id, StateType.Candidate, leaderId );
		
		LOGGER.info("self={}, became candidate at current term {}", this.id, this.term);
	}
	
	private void becomePreCandidate() throws RaftException {		
		// TODO(xiangli) remove the panic when the raft implementation is stable
		if( this.state == StateType.Leader ) 
			 throw new Errors.RaftException("invalid transition [leader -> pre-candidate]");
		//
		// preVote不会递增term，也不会先进行投票，而是等preVote结果出来再进行决定
		//
		this.votes.clear();
		
		this.state = StateType.PreCandidate;
		this.listener.onStateChange( id, StateType.PreCandidate, leaderId );
		LOGGER.info("self={}, became pre-candidate at current term {}", this.id, this.term);
	}
	
	void becomeLeader() throws RaftException {	
		//
		// TODO(xiangli) remove the panic when the raft implementation is stable
		if(this.state == StateType.Follower) 
			throw new Errors.RaftException("invalid transition [follower -> leader]");

		this.reset(  this.term );
		
		this.leaderId = this.id;
		this.state = StateType.Leader;
		this.listener.onStateChange(id, StateType.Leader, leaderId );
		
		
        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        this.getPrs().get(id).becomeReplicate();
        //
		
		// Conservatively set the pendingConfIndex to the last index in the
		// log. There may or may not be a pending config change, but it's
		// safe to delay any future proposals until we commit all our
		// pending log entries, and scanning the entire tail of the log
		// could be expensive.
		this.pendingConfIndex = raftLog.lastIndex();
		
		//
		List<Entry> emptyEnts = Arrays.asList( Entry.getDefaultInstance() );
		if ( !this.appendEntry( emptyEnts ) ) {
			// This won't happen because we just called reset() above.
			LOGGER.error("empty entry was dropped");
		}
		
		LOGGER.info("self={}, became leader at current term {}", this.id, this.term);
	}
	
	//
	private int numOfPendingConf(List<Entry> ents) {		
		if ( Util.isEmpty(ents) ) 
			return 0;
		
		int n = 0;
		for (int i = 0; i < ents.size(); i++) {
			if (ents.get(i).getEntryType() == EntryType.EntryConfChange) 
				n++;
		}
		return n;
	}

	// 竞选
	void campaign(CampaignType campaignType) throws RaftException {
		//
		MessageType voteMsg = null;
		long newTerm = Const.ZERO_TERM; 
		if ( campaignType == CampaignType.CAMPAIGN_PRE_ELECTION ) {
			//
			becomePreCandidate();
			//
			voteMsg = MessageType.MsgRequestPreVote; 
			// PreVote RPCs are sent for the next term before we've incremented r.Term.
			newTerm = this.term + 1;
			
		} else {
			// 调用 becomeCandidate 进入 candidate 状态
			becomeCandidate();
			//
			voteMsg = MessageType.MsgRequestVote;
			newTerm = this.term;
		}
		
		// 调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
		if (quorum() == this.poll(id, Util.voteRespMsgType(voteMsg), true)) {
			// 有半数投票，说明赢得了选举，切换到下一个状态
			if ( campaignType == CampaignType.CAMPAIGN_PRE_ELECTION ) {
				campaign( CampaignType.CAMPAIGN_ELECTION );
			} else {
				// 如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
				becomeLeader();
			}
			return;
		}

		// 向集群里的其他节点发送投票消息
		for(long pid : this.prs.mut()) {
			//
			// 过滤掉自己
			if ( pid == this.id ) 
				continue;
			//
			LOGGER.info("self={}, [last logterm: {}, last index: {}] send {} request to node={} at term {}",
					this.id, raftLog.lastTerm(), raftLog.lastIndex(), voteMsg, pid, this.term);			
			//
			Message.Builder msgBuilder = Message.newBuilder();
			msgBuilder.setTerm(newTerm);
			msgBuilder.setMsgType(voteMsg);
			msgBuilder.setTo( pid );
			msgBuilder.setIndex(raftLog.lastIndex());
			msgBuilder.setLogTerm(raftLog.lastTerm());
			
			if( campaignType == CampaignType.CAMPAIGN_TRANSFER ) 
				msgBuilder.setContext( ZeroByteStringHelper.wrap( campaignType.getValue() ) );

			Message msg = msgBuilder.build();
			send( msg );
		}		
	}
	
	// 轮询集群中所有节点，返回一共有多少节点已经进行了投票
	int poll(long id, MessageType type, boolean v) {
		if (v) {
			LOGGER.info("self={}, received {} from node={} at current term {}", this.id, type, id, this.term);
		} else {
			LOGGER.info("self={}, received {} rejection from node={} at current term {}", this.id, type, id, this.term);
		}

		// 如果id没有投票过，那么更新id的投票情况
		if ( !votes.containsKey(id) ) 
			votes.put(id, v);

		// 计算下都有多少节点已经投票给自己了
		int granted = 0;
		for (boolean flag: votes.values()) {
			if (flag)
				granted++;
		}
		return granted;
	}
	
	private Object _higherTermLock = new Object();
	
	// raft的状态机
	//
	public void step(Message m) throws RaftException {
		
		// Handle the message term, which may result in our stepping down to a follower.
		MessageType msgType = m.getMsgType();
		if ( m.getTerm() == Const.ZERO_TERM ) {
			// 来自本地的消息
		} else if (m.getTerm() > this.term ) {
			
			// 消息的Term大于节点当前的Term
			if( msgType == MessageType.MsgRequestVote || msgType == MessageType.MsgRequestPreVote ) {
				//
				// 如果收到的是投票类消息, 当 context为 campaignTransfer 时表示强制要求进行竞选
				CampaignType compaignType = CampaignType.valuesOf( ZeroByteStringHelper.getByteArray( m.getContext() ) );
				boolean force =  compaignType != null && compaignType.isCampaignTransfer(); 
				
				// 是否在租约期以内
				boolean inLease = checkQuorum && this.leaderId != Const.None && electionElapsed < electionTimeout;
				if( !force && inLease ) {
					// 如果非强制，而且又在租约期以内，就不做任何处理
					// 非强制又在租约期内可以忽略选举消息，见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
					LOGGER.info("self={}, [last term:{}, index:{}, vote:{}] ignored {} vote from node={} [term:{}, "
							+ " index:{}] at current term {}: lease is not expired  (remaining ticks: electionTimeout={}, electionElapsed={})",
							this.id, raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getMsgType(), m.getFrom(), m.getLogTerm(),
							m.getIndex(), this.term, this.electionTimeout, this.electionElapsed);
					return;
				}
			}
			
			if (msgType == MessageType.MsgRequestPreVote || (msgType == MessageType.MsgRequestPreVoteResponse && !m.getReject())) {
				// 在应答一个preVote消息时, 不对任期term做修改
				
			} else {	
				// fixed
				/*
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgHeartbeat message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgHeartbeat message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgHeartbeat message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgHeartbeat message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgAppend message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.Raft - self=111, [current term: 6] received a MsgAppend message with higher term from node=113 [term: 7]
				2019.04.19 03:41:34 com.feeyo.raft.RaftServer - onStateChange, id=111, newStateType=Follower, leaderId=113
				2019.04.19 03:41:34 com.feeyo.raft.RaftServer - onStateChange, id=111, newStateType=Follower, leaderId=113
				2019.04.19 03:41:34 com.feeyo.raft.RaftServer - onStateChange, id=111, newStateType=Follower, leaderId=113
				 */
				//
				synchronized ( _higherTermLock ) {
					//
					if (m.getTerm() > this.term) {
						//
						LOGGER.info("self={}, [current term: {}] received a {} message with higher term from node={} [term: {}]", 
								this.id, this.term, m.getMsgType(), m.getFrom(), m.getTerm());
						
						if ( msgType == MessageType.MsgAppend 
								|| msgType == MessageType.MsgHeartbeat
								|| msgType == MessageType.MsgSnapshot) {					
							// 变成 follower 状态
							becomeFollower(m.getTerm(), m.getFrom());
							
						} else {					
							// 变成 follower 状态
							becomeFollower(m.getTerm(), Const.None);
						}
					}
				}
				
			}
	
		} else if (m.getTerm() < this.term) {

			// 消息的Term小于节点自身的Term，同时消息类型是心跳消息或者是append消息
			if ((this.checkQuorum || this.preVote)
					&& (msgType == MessageType.MsgHeartbeat 
					|| msgType == MessageType.MsgAppend)) {
		
				// 收到了一个节点发送过来的更小的term消息
				// 	这种情况可能是因为消息的网络延时导致，但是也可能因为该节点由于网络分区导致了它递增了term到一个新的任期,
				//  这种情况下该节点不能赢得一次选举，也不能使用旧的任期号重新再加入集群中
				// 	 	如果checkQurom为false，这种情况可以使用递增任期号应答来处理, 但是如果checkQurom为True， 
				//		此时收到了一个更小的term的节点发出的HB或者APP消息，于是应答一个appresp消息，试图纠正它的状态
				
				Message msg = Message.newBuilder()
						.setTo(m.getFrom())
						.setMsgType(MessageType.MsgAppendResponse)
						.build();
                send( msg );
                
			} else if (msgType == MessageType.MsgRequestPreVote) {
				
				// Before pre_vote enable, there may be a recieving candidate with higher term,
                // but less log. After update to pre_vote, the cluster may deadlock if
                // we drop messages with a lower term.
				LOGGER.info("self={}, [last logterm: {}, last index: {}, vote: {}] rejected {} from node={} [logterm: {}, index: {}] at current term {}",
						id, raftLog.lastTerm(), raftLog.lastIndex(), this.vote, msgType, m.getFrom(), m.getLogTerm(), m.getIndex(), this.term);
				
				Message msg = Message.newBuilder()
						.setTo(m.getFrom())
						.setMsgType(MessageType.MsgRequestPreVoteResponse)
						.setTerm(this.term)
						.setReject(true)
						.build();
                send( msg );
                
			} else {				
				// 除了上面的情况以外，忽略任何term小于当前节点所在任期号的消息
				LOGGER.info("self={}, [current term: {}] ignored a {} message with lower term from node={} [term: {}]", this.id,
						this.term, msgType, m.getFrom(), m.getTerm());
			}
			// 在消息的term小于当前节点的term时，不往下处理直接返回了
			return;
		}
				
		//
		switch(msgType) {
		case MsgHup:
			// 收到 MsgHup消息，说明准备进行选举, ( 注：Election Timeout 构造 MsgHup 消息发送给自己 )
			if (this.state != StateType.Leader) { 
				// 当前不是leader
				
				// 取出[applied+1,committed+1]之间的消息，即得到还未进行applied的日志列表
				List<Entry> ents = this.raftLog.slice(this.raftLog.getApplied() + 1, raftLog.getCommitted() + 1, Long.MAX_VALUE);
				
				// 如果其中有config消息，并且commited > applied，说明当前还有没有apply的config消息，这种情况下不能开始投票
				int num = numOfPendingConf(ents);
				if (num != 0 && this.raftLog.getCommitted() > this.raftLog.getApplied()) {
					LOGGER.warn("self={}, cannot campaign at current term {} since there are still {} pending configuration changes to apply",
							this.id, this.term, num);
					return;
				}

				LOGGER.info("self={}, is starting a new election at current term {}", this.id, this.term);

				// 进行选举
				if (preVote) {
					campaign( CampaignType.CAMPAIGN_PRE_ELECTION );	
				} else {
					campaign( CampaignType.CAMPAIGN_ELECTION );
				}
				
			} else {
				LOGGER.debug("self={}, ignoring MsgHup because current is already leader", this.id);
			}
			break;
			
		case MsgRequestVote:
		case MsgRequestPreVote:
			
			// 收到投票类的消息
			if( this.isLearner ) {
				// TODO: learner may need to vote, in case of node down when confchange.
				LOGGER.info("self={}, [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: learner can not vote",
						this.id, raftLog.lastTerm(), raftLog.lastIndex(), this.vote, m.getMsgType(), m.getMsgType(), m.getLogTerm(), m.getIndex(), this.term);
				return;
			}
			
			// 
			synchronized (_requestVoteLock) {

				boolean canVote = this.vote == m.getFrom() || 		
								  (this.vote == Const.None && this.leaderId == Const.None) ||		
								  (msgType == MessageType.MsgRequestPreVote && m.getTerm() > this.term);	
				//
				if (canVote && this.raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {					
					
					// 如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
					// 或者是之前已经投票的节点（r.Vote == m.From）
					// 同时还满足该节点的消息是最新的（r.raftLog.isUpToDate(m.Index, m.LogTerm)），那么就接收这个节点的投票
					
					LOGGER.info("self={}, [last logterm: {}, last index: {}, vote: {}] support {} from node={} [logterm: {}, index: {}] at current term {}",
							this.id, raftLog.lastTerm(), raftLog.lastIndex(), this.vote, 
							m.getMsgType(), m.getFrom(), m.getLogTerm(), m.getIndex(), this.term);
					
					Message msg = Message.newBuilder()
						.setTo(m.getFrom())
						.setMsgType( Util.voteRespMsgType(m.getMsgType()) )
						.setTerm(this.term)
						.setReject(false)
						.build();
					
	                send( msg );
	                
					if (msgType == MessageType.MsgRequestVote) {
						// 保存,是下来给哪个节点投票了
						this.electionElapsed = 0;
						this.vote = m.getFrom();
					}
	                
				} else {
					LOGGER.info("self={}, [last logterm: {}, last index: {}, vote: {}] rejected {} from node={} [logterm: {}, index: {}] at current term {}",
							this.id, raftLog.lastTerm(), raftLog.lastIndex(), this.vote, 
							m.getMsgType(), m.getFrom(), m.getLogTerm(), m.getIndex(), this.term);
					
					// 否则拒绝投票
					Message msg = Message.newBuilder()
							.setTo(m.getFrom())
							.setMsgType( Util.voteRespMsgType(m.getMsgType()) )
							.setTerm(this.term)
							.setReject(true)
							.build();
	                send( msg );
				}
			}
			//
			break;
		default:
			//
			// 其他情况下进入各种状态下自己定制的状态机函数
			nextStep(m);
		}
	}
	
	// 
	private void nextStep(Message m) throws RaftException {
		//
		switch (state) {
		case Follower:
			StepFollower.step(this, m);
			break;
		case PreCandidate:
		case Candidate:
			StepCandidate.step(this, m);
			break;
		case Leader:
			StepLeader.step(this, m);
			break;
		default:
			break;
		}
	}
	
	// 添加日志
	void handleAppendEntries(Message msg) throws RaftException {
		//
		// 先检查消息消息的合法性
		if(msg.getIndex() < raftLog.getCommitted()) {
			
			Message appendResponseMsg = Message.newBuilder()
					.setTo(msg.getFrom())
					.setMsgType(MessageType.MsgAppendResponse)
					.setIndex( raftLog.getCommitted() )
					.build();
			send( appendResponseMsg );
			return;
		}
	
		// 调用 raftLog 的 maybeAppend， 尝试添加到日志模块中，
		// 判断日志条目是否需要Append, 如果没有冲突可以Append， raftLog也会相应的更新自己committed值
		long mlastIndex = raftLog.maybeAppend(msg.getIndex(), msg.getLogTerm(), msg.getCommit(), msg.getEntriesList());
		if (mlastIndex != Const.ZERO_IDX) {			
			// 添加成功
			Message appendResponseMsg = Message.newBuilder()
					.setTo(msg.getFrom())
					.setMsgType(MessageType.MsgAppendResponse)
					.setIndex(mlastIndex)
					.build();
			send( appendResponseMsg );
			
		} else {
			
			// 如果Append失败，则证明不包含匹配LogTerm的Index所对应的条目，通常该情况为节点挂掉一段时间，落后leader节点
			// 则发送reject的回复给leader，并告诉leader当前自身的lastIndex。leader会重新发包含较早的prevLogTerm及prevLogIndex的RPC给该节点
			if ( LOGGER.isDebugEnabled() ) {
				//
				LOGGER.debug("self={}, handle append entries but match term err, current locate msgindex:{} in logterm:{}, "
						+ " raftLog:{}, rejected MsgAppend [logterm:{}, index:{}, ents:{}] from node={}",
						this.id, msg.getIndex(), raftLog.zeroTermOnErrCompacted(msg.getIndex()), 
						raftLog.toString(),
						msg.getLogTerm(), msg.getIndex(), msg.getEntriesCount(), msg.getFrom());
			}
			

			// 添加失败
			Message appendResponseMsg = Message.newBuilder()
					.setTo(msg.getFrom())
					.setMsgType(MessageType.MsgAppendResponse)
					.setIndex(msg.getIndex())
					.setReject(true)
					.setRejectHint(raftLog.lastIndex())
					.build();
			send( appendResponseMsg );
		}
	}

	// follower 或 candidate 收到 MsgHeartbeat，
	// 1、回复 MsgHeartbeatResponse 应答 leader
	// 2、更改自身 committed
	//
	void handleHeartbeat(Message msg) throws RaftException {	
		//
		// 注： 要把 heartbeat 消息 带过来的 context 原样返回
		Message.Builder responseMsg = Message.newBuilder()
				.setTo(msg.getFrom())
				.setMsgType(MessageType.MsgHeartbeatResponse)
				.setContext(msg.getContext());
		try {
			this.raftLog.commitTo( msg.getCommit() );
			//
		} catch(Errors.RaftException ex) {
			//
			LOGGER.warn("self={}, follower commitTo err:  ex={}, msg={}", 
					this.id, ExceptionUtils.getStackTrace(ex), ProtobufUtils.protoToJson(msg));
			//
			responseMsg.setReject(true);
			responseMsg.setRejectHint(raftLog.lastIndex());
		}	
		//
		send( responseMsg.build() );
		
		//
		listener.onReceivedHeartbeat( msg );
	}

	public void handleSnapshot(Message msg) throws RaftException {
		//
		long sindex = msg.getSnapshot().getMetadata().getIndex();
		long sterm = msg.getSnapshot().getMetadata().getTerm();
		
		// 注意这里成功与失败，只是返回的Index参数不同
		if( this.restore( msg.getSnapshot() ) ) {
			
			LOGGER.info("self={}, [commit: {}] restored snapshot [index: {}, term: {}]",
					this.id, raftLog.getCommitted(), sindex, sterm);
			
			Message appendResponseMsg = Message.newBuilder()
					.setTo(msg.getFrom())
					.setMsgType(MessageType.MsgAppendResponse)
					.setIndex(raftLog.lastIndex())
					.build();
			send( appendResponseMsg );
			
		} else {
			LOGGER.info("self={}, [commit: {}] ignored snapshot [index: {}, term: {}]",
					this.id, raftLog.getCommitted(), sindex, sterm);
		
			Message appendResponseMsg = Message.newBuilder()
					.setTo(msg.getFrom())
					.setMsgType(MessageType.MsgAppendResponse)
					.setIndex(raftLog.getCommitted())
					.build();
			send( appendResponseMsg );
		}
	}

	// 使用快照数据进行恢复， 主要是恢复日志及状态机的配置
	private boolean restore(Snapshot snapshot) throws RaftException {
		
		SnapshotMetadata snapMeta = snapshot.getMetadata();
		long snapIndex = snapMeta.getIndex();
		long snapTerm = snapMeta.getTerm();
		ConfState snapCs = snapMeta.getConfState();
		
		// 首先判断快照索引的合法性
		if( snapIndex < this.raftLog.getCommitted() )  {
			LOGGER.info("self={}, snapIndex={}, raftLog.committed={}", this.id, snapIndex, this.raftLog.getCommitted() );
			return false;
		}
				 
		// matchTerm返回true，说明本节点的日志中已经有对应的日志了
		if( this.raftLog.matchTerm(snapIndex, snapTerm) ) {
			
			LOGGER.info("self={}, [last logterm: {}, last index: {}, commit: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
					this.id, raftLog.lastTerm(), raftLog.lastIndex(), raftLog.getCommitted(), snapIndex, snapTerm);
			
			// 提交到快照所在的索引
			this.raftLog.commitTo( snapIndex );
			return false;
		}
		
		// 1. empty learner (just restored) should accept snapshot
		// 2. prevent normal peer from becoming learner from snapshot
		if ((!prs.voters().isEmpty() || !prs.learners().isEmpty()) && !isLearner) {
			for (Long id: snapCs.getLearnersList()) {
				if (id == this.id) {
					LOGGER.error("self={} can't become learner when restores snapshot [index: {}, term: {}]", this.id, snapIndex, snapTerm);
					return false;
				}
			}
		}
		
		LOGGER.info("self={}, [last logterm: {}, last index: {}, commit: {}] starts to restore snapshot [index: {}, term: {}]",
				this.id, raftLog.lastTerm(), raftLog.lastIndex(), raftLog.getCommitted(), snapIndex, snapTerm);
		
		// 恢复，使用快照元数据
		this.raftLog.restore( snapMeta );
		//
		List<Long> nodes = snapCs.getNodesList();
		List<Long> learners = snapCs.getLearnersList();
		//
		this.prs = new ProgressSet(nodes.size(), learners.size());
		this.restoreNode(nodes,false);
		this.restoreNode(learners, true);
		return true;
	}
	
	private void restoreNode(List<Long> nodes, boolean isLearner) throws RaftException {
		for(Long n : nodes) {
			long matched = 0;
			long nextIndex = raftLog.lastIndex() + 1;
			if(n == this.id) {
				matched = nextIndex - 1;
				this.isLearner = isLearner;
			}
			setProgress(n, matched, nextIndex, isLearner);
			LOGGER.info("self={}, restored progress of node={} [{}]", this.id, n, prs.get(n));
		}
	}

	// 返回是否可以被提升为leader
	public boolean promotable() {
		// 在prs数组中找到本节点，说明该节点在集群中，这就具备了可以被选为leader的条件
		return prs.voters().containsKey( id );
	}
	
	// 增加一个节点
	public void addVoterOrLearner(long id, boolean isLearner) throws RaftException {
		
		// 检查是否已经存在节点列表中
		if (prs.voters().containsKey(id)) {
			if (isLearner) {
				LOGGER.info("{} ignored add learner: do not support changing node={} from voter to learner",  this.id,id);
			}
			// Ignore redundant add voter.
			return;
		} else if (prs.learners().containsKey(id)) {
			if (isLearner) {
				// Ignore redundant add learner.
				return;
			}
			prs.promoteLearner(id);
			if (id == this.id) {
				this.isLearner = false;
			}
			
		} else {
			// new progress
			long lastIndex = raftLog.lastIndex();
			
			// 这里才真的添加进来
			this.setProgress(id, 0, lastIndex + 1, isLearner);
		}
		
		// When a node is first added/promoted, we should mark it as recently active.
        // Otherwise, check_quorum may cause us to step down if it is invoked
        // before the added node has a chance to commuicate with us.
		prs.get(id).setRecentActive(true);
	}
	
    // Removes a node from the raft.
	public void removeNode(long nid) throws RaftException {
		//
		delProgress(nid);
		//
		if (prs.voters().isEmpty() && prs.learners().isEmpty()) {
			return;
		}
		
		// 由于删除了节点，所以半数节点的数量变少了，于是去查看是否有可以认为提交成功的数据
		if ( maybeCommit() ) {
			bcastAppend();
		}

		if (this.state == StateType.Leader && leadTransferee == nid) {
			// 如果在leader迁移过程中发生了删除节点的操作，那么中断迁移leader流程
			abortLeaderTransfer();
		}
	}
	
	// ApplyConfChange applies a config change to the local node.
	public void applyConfChange(ConfChange cc) throws RaftException {
		//
		if ( cc.getNodeId() == Const.None ) {
			return;
		}
		//
		switch(cc.getChangeType()) {
		case AddNode:
			addVoterOrLearner(cc.getNodeId(), false);
			break;
		case AddLearnerNode:
			addVoterOrLearner(cc.getNodeId(), true);
			break;
		case RemoveNode:
			removeNode(cc.getNodeId());
			break;
		default:
			throw new Errors.RaftException("unexpected conf type");
		}
	}
	
	private void delProgress(long id) {
		prs.remove(id);
	}
	
	private void setProgress(long id, long matched, long nextIdx, boolean isLearner) throws RaftException {
		Progress pr = new Progress(nextIdx, new Inflights(maxInflight), isLearner);
		pr.setMatched(matched);
		
		if (isLearner) {
			prs.insertLearner(id, pr);
		} else {
			prs.insertVoter(id, pr);
		}
	}
	
    // pastElectionTimeout returns true iff r.electionElapsed is greater
    // than or equal to the randomized election timeout in
    // [electiontimeout, 2 * electiontimeout - 1]
	//
	boolean pastElectionTimeout() {
		return this.electionElapsed > this.randomizedElectionTimeout;
	}
	
	private void resetRandomizedElectionTimeout() {
		this.randomizedElectionTimeout = this.minElectionTimeout 
				+ ThreadLocalRandom.current().nextInt( this.maxElectionTimeout );
	}
	
	// 检查多数节点是否存活 
	boolean checkQuorumActive() {
		int act = 0;
		for (long pid : prs.mut()) {
			//
			if (pid == this.id) {
				act += 1;
				continue;
			}
			//
			Progress pr = prs.get(pid);
			if (!pr.isLearner() && pr.isRecentActive()) {
				act += 1;
			}
			pr.setRecentActive(false);
		}
		return act >= quorum();
	}
	
	void sendTimeoutNow(long to) throws RaftException {
		Message msg = Message.newBuilder()
				.setTo(to)
				.setMsgType(MessageType.MsgTimeoutNow)
				.build();
		send(msg);
	}
	
	void abortLeaderTransfer() {
		leadTransferee = Const.None;
	}

	public ProgressSet getPrs() {
		return prs;
	}

	public ConcurrentLinkedQueue<Message> getMsgs() {
		return msgs;
	}

	public RaftLog getRaftLog() {
		return raftLog;
	}
	
	public long getId() {
		return id;
	}
	
	public long getLeaderId() {
		return leaderId;
	}
	
	public long getTerm() {
		return term;
	}
}