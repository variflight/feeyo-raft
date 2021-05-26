package com.feeyo.raft;

import java.util.List;

/*
 *  Config contains the parameters to start a raft.
 *  
 *  @see https://github.com/tikv/raft-rs/blob/master/src/config.rs
 */
public final class Config {
	//
	private long id;		   // 节点的 id，不能为0
	private List<Long> voters; //
	private List<Long> learners; //
	//
	private int electionTick; // follower 在 election_tick 的时间之后还没有收到 leader 发过来的消息，那么就会重新开始选举
	private int heartbeatTick; // leader 每隔 hearbeat_tick 的时间，都会给 follower 发送心跳消息。默认 10
	//
	private long applied; // applied 是上一次已经被 applied 的 log index	
	private long maxSizePerMsg; // 限制每次发送的最大 message size。默认 1MB
	private int maxInflightMsgs; // 限制复制时候最大的 in-flight 的 message 的数量。默认 256
	private boolean checkQuorum; // leader 每隔 electionTimeout 进行一次 check quorum, 通过通信来判断节点活跃情况
	private boolean preVote; // 是否正常选举之前，进行Pre-Vote
	//
	private int minElectionTick; // election timeout 的范围
	private int maxElectionTick;
	private ReadOnlyOption readOnlyOption;	// internal 读取一致性处理模式(Safe mode & Lease mode)
	private LinearizableReadOption linearizableReadOption; // 读请求是否判断 commit的数据，避免读到老数据，造成不一致的问题
	//
	private boolean skipBcastCommit; // 可能会使follower等待更长的时间来 apply an entry. 这个配置可能影响 proposal forwarding 和 follower read
	private String storageDir;
	private int maxLogFileSize;
	
	private long snapCount;		//
	private int snapInterval;	//
	private boolean disableProposalForwarding; // 是否禁止follower 转发过来的客户端 Proposal请求
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
	public void validate() throws Errors.ConfigException {
		if ( id <= 0 ) {
			throw new Errors.ConfigException("cannot use none as id");
			
		} else if (heartbeatTick <= 0) {
			throw new Errors.ConfigException("heartbeat tick must be greater than 0");

		} else if (electionTick < heartbeatTick) {
			throw new Errors.ConfigException("election tick must be greater than heartbeat tick");

		} else if (maxInflightMsgs <= 0) {
			throw new Errors.ConfigException("max inflight messages must be greater than 0");

		} else if (readOnlyOption == ReadOnlyOption.LeaseBased && !checkQuorum) {
			throw new Errors.ConfigException("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
		}
	}
}