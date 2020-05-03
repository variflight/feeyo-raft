package com.feeyo.raft;

import java.util.List;

//
// Config contains the parameters to start a raft.
//
public final class Config {
	
	// ID is the identity of the local raft. ID cannot be 0.
	private long id;
	
	// peers contains the IDs of all voters nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	private List<Long> voters;

	// learners contains the IDs of all learner nodes (including self if the
	// local node is a learner) in the raft cluster. learners only receives
	// entries from the leader node. It does not vote or promote itself.
	private List<Long> learners;

	// 当 follower 在 election_tick 的时间之后还没有收到 leader 发过来的消息，那么就会重新开始选举
	private int electionTick;

	// leader 每隔 hearbeat_tick 的时间，都会给 follower 发送心跳消息。默认 10
	private int heartbeatTick;

	// // applied 是上一次已经被 applied 的 log index
	private long applied;

	// 限制每次发送的最大 message size。默认 1MB
	private long maxSizePerMsg;

	// 限制复制时候最大的 in-flight 的 message 的数量。默认 256
	private int maxInflightMsgs;

	/// Specify if the leader should check quorum activity. Leader steps down
	/// when
	/// quorum is not active for an electionTimeout.
	private boolean checkQuorum;

	/// pre_vote enables the Pre-Vote algorithm described in raft thesis section
	/// 9.6. This prevents disruption when a node that has been partitioned away
	/// rejoins the cluster.
	private boolean preVote;

	/// The range of election timeout. In some cases, we hope some nodes has
	/// less
	/// possibility
	/// to become leader. This configuration ensures that the randomized
	/// election_timeout
	/// will always be suit in [min_election_tick, max_election_tick).
	/// If it is 0, then election_tick will be chosen.
	private int minElectionTick;

	/// If it is 0, then 2 * election_tick will be chosen.
	private int maxElectionTick;

	/// Choose the linearizability mode or the lease mode to read data.
	/// If you don’t care about the read consistency and want a higher read
	/// performance, you can use the lease mode.
	private ReadOnlyOption readOnlyOption;

	/// Linearizable Read通俗来讲，就是读请求需要读到最新的已经commit的数据，不会读到老数据
	/// @see https://aphyr.com/posts/313-strong-consistency-models
	///
	private LinearizableReadOption linearizableReadOption;

	/// Don't broadcast an empty raft entry to notify follower to commit an
	/// entry.
	/// This may make follower wait a longer time to apply an entry. This
	/// configuration
	/// May affect proposal forwarding and follower read.
	private boolean skipBcastCommit;

	private String storageDir;

	private int maxLogFileSize;

	private long snapCount;		//
	private int snapInterval;	//

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way.
	// Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	private boolean disableProposalForwarding;

	private boolean syncLog;
	//

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public List<Long> getVoters() {
		return voters;
	}

	public void setVoters(List<Long> voters) {
		this.voters = voters;
	}

	public List<Long> getLearners() {
		return learners;
	}

	public void setLearners(List<Long> learners) {
		this.learners = learners;
	}

	//
	public int getElectionTick() {
		return electionTick;
	}

	public void setElectionTick(int electionTick) {
		this.electionTick = electionTick;
	}

	public int getHeartbeatTick() {
		return heartbeatTick;
	}

	public void setHeartbeatTick(int heartbeatTick) {
		this.heartbeatTick = heartbeatTick;
	}

	public long getApplied() {
		return applied;
	}

	public void setApplied(long applied) {
		this.applied = applied;
	}

	public long getMaxSizePerMsg() {
		return maxSizePerMsg;
	}

	public void setMaxSizePerMsg(long maxSizePerMsg) {
		this.maxSizePerMsg = maxSizePerMsg;
	}

	public int getMaxInflightMsgs() {
		return maxInflightMsgs;
	}

	public void setMaxInflightMsgs(int maxInflightMsgs) {
		this.maxInflightMsgs = maxInflightMsgs;
	}

	public boolean isCheckQuorum() {
		return checkQuorum;
	}

	public void setCheckQuorum(boolean checkQuorum) {
		this.checkQuorum = checkQuorum;
	}

	public boolean isPreVote() {
		return preVote;
	}

	public void setPreVote(boolean preVote) {
		this.preVote = preVote;
	}

	public int getMinElectionTick() {
		return minElectionTick;
	}

	public void setMinElectionTick(int minElectionTick) {
		this.minElectionTick = minElectionTick;
	}

	public int getMaxElectionTick() {
		return maxElectionTick;
	}

	public void setMaxElectionTick(int maxElectionTick) {
		this.maxElectionTick = maxElectionTick;
	}

	public boolean isSkipBcastCommit() {
		return skipBcastCommit;
	}

	public void setSkipBcastCommit(boolean skipBcastCommit) {
		this.skipBcastCommit = skipBcastCommit;
	}

	public String getStorageDir() {
		return storageDir;
	}

	public void setStorageDir(String storageDir) {
		this.storageDir = storageDir;
	}

	public int getMaxLogFileSize() {
		return maxLogFileSize;
	}

	public void setMaxLogFileSize(int maxLogFileSize) {
		this.maxLogFileSize = maxLogFileSize;
	}

	public long getSnapCount() {
		return snapCount;
	}

	public void setSnapCount(long snapCount) {
		this.snapCount = snapCount;
	}

	public int getSnapInterval() {
		return snapInterval;
	}

	public void setSnapInterval(int snapInterval) {
		this.snapInterval = snapInterval;
	}

	public int minElectionTick() {
		return minElectionTick == 0 ? electionTick : minElectionTick;
	}

	public int maxElectionTick() {
		return maxElectionTick == 0 ? 2 * electionTick : maxElectionTick;
	}

	public ReadOnlyOption getReadOnlyOption() {
		return readOnlyOption;
	}

	public void setReadOnlyOption(ReadOnlyOption readOnlyOption) {
		this.readOnlyOption = readOnlyOption;
	}

	public boolean isDisableProposalForwarding() {
		return disableProposalForwarding;
	}

	public void setDisableProposalForwarding(boolean disableProposalForwarding) {
		this.disableProposalForwarding = disableProposalForwarding;
	}

	public LinearizableReadOption getLinearizableReadOption() {
		return linearizableReadOption;
	}

	public void setLinearizableReadOption(LinearizableReadOption linearizableReadOption) {
		this.linearizableReadOption = linearizableReadOption;
	}

	public boolean isSyncLog() {
		return syncLog;
	}

	public void setSyncLog(boolean syncLog) {
		this.syncLog = syncLog;
	}

	//
	public void validate() throws Errors.RaftConfigException {
		if ( id <= 0 ) {
			throw new Errors.RaftConfigException("cannot use none as id");
			
		} else if (heartbeatTick <= 0) {
			throw new Errors.RaftConfigException("heartbeat tick must be greater than 0");

		} else if (electionTick < heartbeatTick) {
			throw new Errors.RaftConfigException("election tick must be greater than heartbeat tick");

		} else if (maxInflightMsgs <= 0) {
			throw new Errors.RaftConfigException("max inflight messages must be greater than 0");

		} else if (readOnlyOption == ReadOnlyOption.LeaseBased && !checkQuorum) {
			throw new Errors.RaftConfigException(
					"CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
		}
	}

}