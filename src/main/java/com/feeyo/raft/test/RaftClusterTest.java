package com.feeyo.raft.test;

import com.feeyo.raft.Errors.RaftException;

import java.util.concurrent.TimeUnit;

import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.nio.util.timer.HashedWheelTimer;
import com.feeyo.net.nio.util.timer.Timeout;
import com.feeyo.net.nio.util.timer.TimerTask;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.KeyValue;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Newdbpb;
import com.feeyo.raft.proto.Newdbpb.ColumnFamilyHandle;
import com.feeyo.raft.proto.Newdbpb.Operation;
import com.feeyo.raft.proto.Newdbpb.ProposeCmd;
import com.feeyo.raft.test.VirtualNode.SyncWaitCallback;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.UUIDUtil;
import com.google.protobuf.ByteString;

public class RaftClusterTest {
	
	private static ProtobufEncoder protobufEncoder = new ProtobufEncoder(true);
	
	private static Message createMessage(String cbKey, 
			ColumnFamilyHandle cfh,
			byte[] key, byte[] value, 
			Operation operation) {

        KeyValue keyValue = KeyValue.newBuilder() //
                .setKey( ByteString.copyFrom(key) ) //
                .setValue( ByteString.copyFrom(value) ) //
                .build();

		ProposeCmd cmd = ProposeCmd.newBuilder() //
				.addKv(keyValue) //
				.setOp(operation) //
				.setCfh(cfh) //
				.build();
		//
		byte[] data = protobufEncoder.encode(cmd);
		
		Entry entry = Entry.newBuilder() //
				.setEntryType(EntryType.EntryNormal) //
				.setData(ByteString.copyFrom(data)) //
				.setCbkey(cbKey) //
				.build();

        return Message.newBuilder() //
				.setMsgType(MessageType.MsgPropose) //
				.setFrom(1000) //
				.addEntries( entry ) //
				.build();
	}
	
	//
	static VirtualRaftCluster cluster = null;
	static HashedWheelTimer wheelTimer = new HashedWheelTimer(new NamedThreadFactory("timer-"), 50, TimeUnit.MILLISECONDS, 4096);
	
	private static void nextWrite() {
		//
		// 延迟 write
		wheelTimer.newTimeout(new TimerTask() {

			@Override
			public void run(Timeout timeout) throws Exception {
				//
				if (cluster != null) {
					//
					int count = 2;
					long sum = 0;
					//
					for (int i = 0; i < count; i++) {
						long startMs = System.currentTimeMillis();
						//
						String cbKey = UUIDUtil.getUuid();
						Message message = createMessage(cbKey, Newdbpb.ColumnFamilyHandle.DefaultCfh,
								new byte[] { 1, 2 }, new byte[] { 3, 4 }, Newdbpb.Operation.Insert);
						//
						SyncWaitCallback c = new SyncWaitCallback();
						c.cbKey = cbKey;
						cluster.syncWait(message, c);
						c.await();
						sum += System.currentTimeMillis() - startMs;
					}

					System.out.println("sum=" + sum + ", count=" + count);
				}
				//
				nextWrite();
			}

		}, 15, TimeUnit.SECONDS);
	}
	
	private static volatile long killId = 0;
	
	private static void nextRandomStartAndStop() {
		//
		// 随机 stop & start
		wheelTimer.newTimeout(new TimerTask() {
			@Override
			public void run(Timeout timeout) throws Exception {
				//
				if (cluster != null) {
					
					if ( killId == 0 ) {
						killId = cluster.leaderId;
						cluster.stopById( killId );
						System.out.println("##stop node, id=" + killId);
					} else {
						System.out.println("##start node, id=" + killId);
						cluster.startById( killId );
						killId = 0;
					}
				}
				//
				nextRandomStartAndStop();
			}

		}, 90, TimeUnit.SECONDS);
	}
	
	public static void main(String[] args) throws RaftException {
		//
		// SET LOGGER LEVEL
		org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
		logger4j.setLevel(org.apache.log4j.Level.toLevel("DEBUG")); // DEBUG INFO ERROR
		
		if ( cluster == null )
			cluster = new VirtualRaftCluster();
		//
		long startWaitMs = System.currentTimeMillis();
		for(;;) {
			//
			if ( cluster.leaderId > 0 ) {
				System.out.println("wait leader=" + (System.currentTimeMillis() - startWaitMs ) );
				break;
			}
			//
			try {
				Thread.sleep(1L);
			} catch (InterruptedException e) {
			}
		}
		//
		nextWrite();
		//
		nextRandomStartAndStop();
		
	}
	
	
}
