package com.feeyo.raft.transport.test;

import java.io.IOException;

import com.feeyo.net.nio.NetConfig;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.buffer.BufferPool;
import com.feeyo.buffer.bucket.BucketBufferPool;
import com.feeyo.net.nio.util.ExecutorUtil;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Peer;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotChunk;
import com.feeyo.raft.transport.TransportClient;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.StandardThreadExecutor;
import com.google.protobuf.ByteString;

public class TransportClientTest {
	
	
	public static void main(String[] args) throws IOException {
		
		// 初始化 network & buffer 配置
        long minBufferSize = 52428800;
        long maxBufferSize =  104857600;
        int[] chunkSizes = new int[]{1024, 2048, 4096, 8192};

        int tpCoreThreads = 5;
        int tpMaxThreads = 50;
        int tpQueueCapacity = 10;
        int timerSize = 2;

        int socketSoRcvbuf =  32768;
        int socketSoSndbuf = 32768;
		
		BufferPool bufferPool = new BucketBufferPool(minBufferSize, maxBufferSize, chunkSizes);
		
        // 构造 boss & time threadPool
        //
        new NetSystem(bufferPool, 
        		new StandardThreadExecutor(tpCoreThreads, tpMaxThreads, tpQueueCapacity, new NamedThreadFactory("Boss/threadPool-", true)), 
        		ExecutorUtil.create("TimerExecutor-", timerSize),
        		ExecutorUtil.createScheduled("TimerSchedExecutor-", 2));
        NetSystem.getInstance().setNetConfig(new NetConfig(socketSoRcvbuf, socketSoSndbuf));
        
		//
		PeerSet peerSet = new PeerSet();
		peerSet.put( new Peer(1, "127.0.0.1", 8080, false) );
		//
		
		TransportClient transportClient = new TransportClient();
		transportClient.start(peerSet);
		
		byte[] bb = new byte[1024];
		for(int i = 0; i < bb.length; i++)
			bb[i] = 11;
		
		SnapshotChunk chunk = SnapshotChunk.newBuilder()
				.setData( ByteString.copyFrom( bb ) )
				.build();
		Snapshot snapshot = Snapshot.newBuilder()
				.setChunk( chunk )
				.build();
		
		Message msg = Message.newBuilder()
				.setTo(1)
				.setCommit(1)
				.setIndex(22)
				.setMsgType( MessageType.MsgSnapshot )
				.setSnapshot( snapshot )
				.build();
		
		long sum = 0;
	
		for( int i =0; i < 10000; i++) {
			long t1 = TimeUtil.currentTimeMillis();
			transportClient.pipeliningPost(msg);
			long t2 = TimeUtil.currentTimeMillis();
			sum += ( t2 - t1);
		
		}
		
		System.out.println( "sum=" + sum );
		
		//transportClient.stop();
		
	}

}
