# Raft library

Raft is a protocol with which a cluster of nodes can maintain a replicated state machine.
The state machine is kept in sync through the use of a replicated log.
Thanks to the etcd(https://github.com/etcd-io/etcd/tree/main/raft) and raft-rs(https://github.com/tikv/raft-rs) projects.
Because our Java implementation refers to their raft architecture.


## Features

This raft implementation is a full feature implementation of Raft protocol. Features includes:

- Leader election
- Log replication
- Log compaction
- Membership changes
- Leadership transfer extension
- Efficient linearizable read-only queries served by both the leader and followers
  - leader checks with quorum and bypasses Raft log before processing read-only queries
  - followers asks leader to get a safe read index before processing read-only queries
- More efficient lease-based linearizable read-only queries served by both the leader and followers
  - leader bypasses Raft log and processing read-only queries locally
  - followers asks leader to get a safe read index before processing read-only queries
  - this approach relies on the clock of the all the machines in raft group

This raft implementation also includes a few optional enhancements:

- Optimistic pipelining to reduce log replication latency
- Flow control for log replication
- Batching Raft messages to reduce synchronized network I/O calls
- Batching log entries to reduce disk synchronized I/O
- Writing to leader's disk in parallel
- Internal proposal redirection from followers to leader
- Automatic stepping down when the leader loses quorum
- Protection against unbounded log growth when quorum is lost


## Usage

```
	<dependency>
	  <groupId>com.github.variflight</groupId>
	  <artifactId>feeyo-raft</artifactId>
	  <version>0.1.0</version>
	</dependency>
```

How to use a single raft

```
    //
    // Set the raft.xml configuration
    //
    Peer localPeer = null;
    PeerSet peerSet = new PeerSet();
    Peer peer1 = new Peer(111, "192.168.1.2", 8888, false, 80);
    Peer peer2 = new Peer(111, "192.168.1.3", 8888, false, 100);
    Peer peer3 = new Peer(111, "192.168.1.4", 8888, false, 120);
    peerSet.put(peer1);
    peerSet.put(peer2);
    peerSet.put(peer3);
    localPeer = peer1;
    
    RaftConfig raftCfg = RaftConfigLoader.load("raft.xml");
    raftCfg.setLocalPeer(localPeer);
    raftCfg.setPeerSet(peerSet);
    
    StateMachine stateMachine = new StateMachineAdapter() {
		@Override
		public void initialize(long committed) {
		}

		@Override
		public void applyMemberChange(PeerSet peerSet, long committed) {
			super.applyMemberChange(peerSet, committed);
		}

		@Override
		public boolean apply(byte[] data, long committed) {
		}

		@Override
		public void applySnapshot(boolean toCleanup, byte[] data) {
		}

		@Override
		public void takeSnapshot(SnapshotHandler handler) throws RaftException {
		}

		@Override
		public void leaderChange(long leaderId) {
			super.leaderChange(leaderId);
		}

		@Override
		public void leaderCommitted(long leaderCommitted) {
		}
	};
	
    final int raftReactorSize = 4;
    final RaftServer raftServer = new RaftServerFastImpl(raftCfg, stateMachine);
    raftServer.start(raftReactorSize);   
```

How to use mutil-raft

```
	private static RaftGroupServer make(Peer local, PeerSet peerSet, List<Region> regions) {
		Config c = new Config();
		c.setElectionTick(50);
		c.setHeartbeatTick(10);
		c.setApplied(0);
		c.setMaxSizePerMsg(1024 * 1024);
		c.setMaxInflightMsgs(256);
		c.setMinElectionTick(0);
		c.setMaxElectionTick(0);
		c.setMaxLogFileSize(10 * 1024 * 1024);
		c.setSnapCount(1000);
		c.setCheckQuorum(true);
		c.setPreVote(true);
		c.setSkipBcastCommit(false);
		c.setStorageDir("/multi_raft/" + local.getId());
		c.setReadOnlyOption(ReadOnlyOption.Safe);
		c.setLinearizableReadOption(LinearizableReadOption.FollowerRead);
		c.setDisableProposalForwarding(false);
		//
		RaftGroupConfig raftGroupCfg = new RaftGroupConfig(c);
		raftGroupCfg.setLocalPeer(local);
		raftGroupCfg.setPeerSet(peerSet);
		raftGroupCfg.setRegions(regions);
		raftGroupCfg.setTpCoreThreads(12);
		raftGroupCfg.setTpMaxThreads(100);
		raftGroupCfg.setTpQueueCapacity(2500);
		return new RaftGroupServer(raftGroupCfg, 
			new RaftGroupStateMachineAdapter(local.getId(), peerSet){
			...
		});
	}
	//
	PeerSet peerSet = new PeerSet();
	Peer peer1 = new Peer(1, "127.0.0.1", 8081, false);
	Peer peer2 = new Peer(2, "127.0.0.1", 8082, false);
	Peer peer3 = new Peer(3, "127.0.0.1", 8083, false);
	peerSet.put( peer1 );
	peerSet.put( peer2 );
	peerSet.put( peer3 );
	//
	List<Region> regions = new ArrayList<>();
	regions.add(createRegion(11, new byte[]{ 0x00000000 }, new byte[] { 0x11 }, 1, 1));
	regions.add(createRegion(12, new byte[]{ 0x12 }, new byte[] { 0x13 }, 1, 1));
	regions.add(createRegion(13, new byte[]{ 0x14 }, new byte[] { 0xffffffff }, 1, 1));
	//
	RaftGroupServer server1 = make(peer1, peerSet, regions);
	RaftGroupServer server2 = make(peer2, peerSet, regions);
	RaftGroupServer server3 = make(peer3, peerSet, regions);
	server1.start(2);
	server2.start(2);
	server3.start(2);
```
