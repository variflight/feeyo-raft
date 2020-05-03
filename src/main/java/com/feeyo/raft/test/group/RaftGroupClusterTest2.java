package com.feeyo.raft.test.group;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.nio.NetConfig;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.buffer.BufferPool;
import com.feeyo.net.nio.buffer.bucket.BucketBufferPool;
import com.feeyo.net.nio.util.ExecutorUtil;
import com.feeyo.raft.*;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.Peer;
import com.feeyo.raft.StateMachine.SnapshotHandler;
import com.feeyo.raft.config.RaftGroupConfig;
import com.feeyo.raft.group.RaftGroup;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.group.RaftGroupStateMachineAdapter;
import com.feeyo.raft.group.proto.Raftgrouppb.*;
import com.feeyo.raft.proto.Newdbpb.ProposeCmd;
import com.feeyo.raft.proto.Raftpb.*;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

//
// Multi-raft test case
public class RaftGroupClusterTest2 {

    private static final ProtobufEncoder protobufEncoder = new ProtobufEncoder(true);
    private static final ProtobufDecoder<ProposeCmd> protobufDecoder = new ProtobufDecoder<>(ProposeCmd.getDefaultInstance(), true);
    private static final Random random = new Random();

    private static RaftGroupServer make(Peer local, PeerSet peerSet, List<Region> regions) {
        //
        Config c = new Config();
        c.setElectionTick(50);
        c.setHeartbeatTick(10);
        c.setApplied(0);
        c.setMaxSizePerMsg(1024 * 1024);
        c.setMaxInflightMsgs(256);
        c.setMinElectionTick(0);
        c.setMaxElectionTick(0);
        c.setMaxLogFileSize(10 * 1024 * 1024);
        c.setSnapCount(999);
        c.setCheckQuorum(true);
        c.setPreVote(true);
        c.setSkipBcastCommit(false);
        c.setStorageDir("/home/hyhe/data/multi_raft_test/" + local.getId());
        c.setReadOnlyOption(ReadOnlyOption.Safe);
        c.setLinearizableReadOption(LinearizableReadOption.FollowerRead);
        c.setDisableProposalForwarding(false);

        RaftGroupConfig raftGroupCfg = new RaftGroupConfig(c);
        raftGroupCfg.setLocalPeer(local);
        raftGroupCfg.setPeerSet(peerSet);
        raftGroupCfg.setRegions(regions);
        raftGroupCfg.setTpCoreThreads(12);
        raftGroupCfg.setTpMaxThreads(100);
        raftGroupCfg.setTpQueueCapacity(2500);
        //
        return new RaftGroupServer(raftGroupCfg, new RaftGroupStateMachineAdapter(local.getId(), peerSet) {

            @Override
            public boolean apply(long regionId, byte[] data, long committed) {
                //
                List<ProposeCmd> commands = protobufDecoder.decode(data);
                if (commands == null) {
                    System.out.println("Empty data " + Arrays.toString(data));
                    return true;
                }
                ProposeCmd msg = commands.get(0);
                KeyValue keyValue = msg.getKv(0);
                System.out.println("Region " + regionId + " apply Key is " + keyValue.getKey().toStringUtf8() + "\t value is " + keyValue.getValue().toStringUtf8());
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

    public static void main(String[] args) throws Throwable {
        //
        BufferPool bufferPool = new BucketBufferPool(1024 * 1024 * 40, 1024 * 1024 * 80, //
                1024 * 16, 1024, new int[]{1024}, 1024 * 32);

        //
        new NetSystem(bufferPool, ExecutorUtil.create("BusinessExecutor-", 2), //
                ExecutorUtil.create("TimerExecutor-", 2), //
                ExecutorUtil.createScheduled("TimerScheduledExecutor-", 1));

        NetConfig systemConfig = new NetConfig(1048576, 4194304);
        NetSystem.getInstance().setNetConfig(systemConfig);

        //
        Peer peer1 = new Peer(1, "127.0.0.1", 8081, false);
        Peer peer2 = new Peer(2, "127.0.0.1", 8082, false);
        Peer peer3 = new Peer(3, "127.0.0.1", 8083, false);

        PeerSet peerSet = new PeerSet();
        peerSet.put(peer1);
        peerSet.put(peer2);
        peerSet.put(peer3);

        List<Peer> peers = new ArrayList<>(peerSet.values());
        List<Region> regions = new ArrayList<>();
        // 手动分区
        fillUpRegion(regions);

        RaftGroupServer server1 = make(peer1, peerSet, regions);
        RaftGroupServer server2 = make(peer2, peerSet, regions);
        RaftGroupServer server3 = make(peer3, peerSet, regions);

        server1.start(2);
        server2.start(2);
        server3.start(2);

        Thread.sleep(12000);

        for (Region region : regions) {
            RaftGroup group = server1.findGroupByRegionId(region.getId());
            RaftGroup group2 = server2.findGroupByRegionId(region.getId());
            RaftGroup group3 = server3.findGroupByRegionId(region.getId());

            System.out.printf("Region (%d), %d/%d/%d\r\n", region.getId(), //
                    group.getLeaderId(), group2.getLeaderId(), group3.getLeaderId());
        }


        // Test 发送消息
        sendGroupMessage(server1, regions, peers);
        sendGroupMessage(server3, regions, peers);
        sendGroupMessage(server2, regions, peers);
        System.out.println("All message finished");
    }

    private static void sendGroupMessage(RaftGroupServer server, List<Region> regions, List<Peer> peers) {
        // 循环发送n条数据
        for (int i = 0; i < 10; i++) {
            server.getRaftCaller().sync(randomRegionMessage(regions, peers));
        }
    }

    private static RaftGroupMessage randomRegionMessage(List<Region> regions, List<Peer> peers) {
        long regionId = regions.get(random.nextInt(regions.size())).getId();
        String key = "Region" + regionId + random.nextInt(100);
        String value = "Region" + regionId;
        long from = peers.get(random.nextInt(peers.size())).getId();
        long to = peers.get(random.nextInt(peers.size())).getId();

        KeyValue keyValue = KeyValue.newBuilder() //
                .setKey(ByteString.copyFromUtf8(key)) //
                .setValue(ByteString.copyFromUtf8(value)) //
                .build();

        ProposeCmd cmd = ProposeCmd.newBuilder() //
                .addKv(keyValue) //
                .build();

        byte[] data = protobufEncoder.encode(cmd);
        Entry entry = Entry.newBuilder() //
                .setEntryType(EntryType.EntryNormal) //
                .setData( ByteString.copyFrom(data) ) //
                .build();

        Message raftMessage = Message.newBuilder() //
                .setMsgType(MessageType.MsgPropose) //
                .setFrom(from) //
                .setTo(to) //
                .addEntries(entry) //
                .build();

        return RaftGroupMessage.newBuilder()//
                .setMessage(raftMessage) //
                .setRegionId(regionId) //
                .build();
    }

    private static void fillUpRegion(List<Region> regions) {
        Region region = Region.newBuilder() //
                .setId(1) //
                .setStartKey(ByteString.copyFromUtf8("Region100")) //
                .setEndKey(ByteString.copyFromUtf8("Region199")) //
                .build();
        regions.add(region);

        region = Region.newBuilder() //
                .setId(2) //
                .setStartKey(ByteString.copyFromUtf8("Region200")) //
                .setEndKey(ByteString.copyFromUtf8("Region299")) //
                .build();
        regions.add(region);

        region = Region.newBuilder() //
                .setId(3) //
                .setStartKey(ByteString.copyFromUtf8("Region300")) //
                .setEndKey(ByteString.copyFromUtf8("Region399")) //
                .build();
        regions.add(region);

        region = Region.newBuilder() //
                .setId(4) //
                .setStartKey(ByteString.copyFromUtf8("Region400")) //
                .setEndKey(ByteString.copyFromUtf8("Region499")) //
                .build();
        regions.add(region);

        region = Region.newBuilder() //
                .setId(5) //
                .setStartKey(ByteString.copyFromUtf8("Region500")) //
                .setEndKey(ByteString.copyFromUtf8("Region599")) //
                .build();
        regions.add(region);
    }
}
