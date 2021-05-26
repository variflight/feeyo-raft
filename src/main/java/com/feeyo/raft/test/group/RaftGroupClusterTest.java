package com.feeyo.raft.test.group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.nio.NetConfig;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.buffer.BufferPool;
import com.feeyo.buffer.bucket.BucketBufferPool;
import com.feeyo.net.nio.util.ExecutorUtil;
import com.feeyo.raft.Config;
import com.feeyo.raft.Const;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.LinearizableReadOption;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.ReadOnlyOption;
import com.feeyo.raft.StateMachine.SnapshotHandler;
import com.feeyo.raft.caller.RaftCaller.Reply;
import com.feeyo.raft.config.RaftGroupConfig;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.group.RaftGroupStateMachineAdapter;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.group.proto.Raftgrouppb.Region;
import com.feeyo.raft.group.proto.Raftgrouppb.RegionEpoch;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.KeyValue;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Newdbpb.ProposeCmd;
import com.google.protobuf.ByteString;

//
// Multi-raft test case
public class RaftGroupClusterTest {
	
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
		c.setStorageDir("/Users/zhuam/git/feeyo/feeyoraft/multi_raft_test/" + local.getId());
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
		//
		return new RaftGroupServer(raftGroupCfg, new RaftGroupStateMachineAdapter(local.getId(), peerSet){
			@Override
			public boolean apply(long regionId, byte[] data, long committed) {
	            List<ProposeCmd> commands = protobufDecoder.decode(data);
                if (commands == null) {
                    System.out.println("Empty data " + Arrays.toString(data));
                    return true;
                }
                //
                System.out.println("Region " + regionId + " "+  commands.size());
                for(int i = 0; i < commands.size(); i++) {
	                ProposeCmd msg = commands.get(0);
	                KeyValue keyValue = msg.getKv(0);
	                System.out.println("Region " + regionId + " "+ keyValue.getKey().toStringUtf8() + " " + keyValue.getValue().toStringUtf8());
                }
				return true;
			}

			@Override
			public void applySnapshot(long regionId, boolean toCleanup, byte[] data) {
			}

			@Override
			public void takeSnapshot(long regionId, SnapshotHandler handler) throws RaftException {
			}
		});
	}
	
	
	//
	// --------------------------------------------------------------------------------
	//
	public static Region createRegion(long id, byte[] startKey, byte[] endKey, long confVer, long version) {
		//
		RegionEpoch regionEpoch = RegionEpoch.newBuilder() //
				.setConfVer(confVer) //
				.setVersion(version) //
				.build(); //
		Region region = Region.newBuilder()	//
				.setId(id)	//
				.setStartKey(ByteString.copyFrom(startKey) )	//
				.setEndKey( ByteString.copyFrom(endKey) )	//
				.setRegionEpoch(regionEpoch)	//
				.build();	//
		return region;
	}
	
	//
	private static final ProtobufEncoder protobufEncoder = new ProtobufEncoder(true);
	private static final ProtobufDecoder<ProposeCmd> protobufDecoder = new ProtobufDecoder<>(ProposeCmd.getDefaultInstance(), true);
	
	private static RaftGroupMessage createMessage(long to, List<KeyValue> kvs) {
		List<Entry> entries = new ArrayList<>();
		for(KeyValue kv: kvs) {
			ProposeCmd cmd = ProposeCmd.newBuilder() //
					.addKv(kv)
					.build();
			//
			byte[] data = protobufEncoder.encode(cmd);
			Entry entry = Entry.newBuilder() //
					.setEntryType(EntryType.EntryNormal) //
					.setData(ByteString.copyFrom(data)) //
					.build();
			
			entries.add(entry);
		}

		Message message = Message.newBuilder() //
				.setMsgType(MessageType.MsgPropose) //
				.setTo(to) //
				.setFrom(Const.None) //
				.addAllEntries( entries ) //
				.build(); 
		//
		return RaftGroupMessage.newBuilder() //
				.setRegionId(11) //
				.setIsTombstone(false) //
				.setMessage(message) //
				.build(); 
	}

	//
	static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
	static void continueWrite(final RaftGroupServer[] servers) {
		//
		// 延迟 write
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				//
				RaftGroupStateMachineAdapter stateMachine = servers[0].getStateMachine();
				long leaderId = stateMachine.getLeaderId(11);
				System.out.println("leaderId=" + leaderId ); 
				
				// find leader
				RaftGroupServer leaderServer = null;
				for(RaftGroupServer tmpServer: servers) {
					if (tmpServer.getId() == leaderId ) {
						leaderServer = tmpServer;
						break;
					}
				}
				
				//
				if ( leaderServer != null ) {
					List<KeyValue> kvs = new ArrayList<>();
					for(int i = 0; i < 10; i++) {
						byte[] key = "hello".getBytes();
						byte[] value = (" world_" +i).getBytes();
						//
						KeyValue keyValue = KeyValue.newBuilder() //
							.setKey(ByteString.copyFrom(key)) //
							.setValue(ByteString.copyFrom(value)) //
							.build();
						//
						kvs.add(keyValue);
					}
					RaftGroupMessage msg = createMessage(leaderId, kvs);
					Reply reply = leaderServer.getRaftCaller().sync( msg );
					System.out.println("reply=" + reply.code + ", " + new String(reply.msg));
				}
			}
			
		}, 15, 15, TimeUnit.SECONDS);
	}
	
	
	public static void main(String[] args) throws Throwable {
		
		//
		// SET LOGGER LEVEL
		org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
		logger4j.setLevel(org.apache.log4j.Level.toLevel("INFO")); // DEBUG INFO ERROR
		
		//
		BufferPool bufferPool = new BucketBufferPool(41943040, 83886080,new int[] { 1024 });
		new NetSystem(bufferPool, ExecutorUtil.create("BusinessExecutor-", 2),ExecutorUtil.create("TimerExecutor-", 2),
				ExecutorUtil.createScheduled("TimerSchedExecutor-", 1));
		//
		NetConfig systemConfig = new NetConfig(1048576, 4194304);
		NetSystem.getInstance().setNetConfig(systemConfig);
		
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
		regions.add(createRegion(11, new byte[] { 0x00000000 }, new byte[] { 0x11 }, 1, 1));
		regions.add(createRegion(12, new byte[] { 0x12 }, new byte[] { 0x13 }, 1, 1));
		regions.add(createRegion(13, new byte[] { 0x14 }, new byte[] { 0xffffffff }, 1, 1));
		//
		RaftGroupServer server1 = make(peer1, peerSet, regions);
		RaftGroupServer server2 = make(peer2, peerSet, regions);
		RaftGroupServer server3 = make(peer3, peerSet, regions);
		server1.start(2);
		server2.start(2);
		server3.start(2);
		//
		continueWrite( new RaftGroupServer[]{ server1, server2, server3 });
	}

}
