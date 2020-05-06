package com.feeyo.raft.transport.test;

import java.io.IOException;
import java.util.concurrent.Executors;

import com.feeyo.net.nio.NetConfig;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.buffer.BufferPool;
import com.feeyo.buffer.bucket.BucketBufferPool;
import com.feeyo.net.nio.util.ExecutorUtil;
import com.feeyo.raft.RaftServer;
import com.feeyo.raft.transport.TransportServer;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.StandardThreadExecutor;

public class TransportServerTest {
	
	//
	public static void main(String[] args) throws IOException {
		
		// 初始化 network & buffer 配置
        long minBufferSize = 52428800;
        long maxBufferSize =  104857600;
        int[] chunkSizes = new int[]{4096};

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
        		new StandardThreadExecutor(tpCoreThreads, tpMaxThreads, tpQueueCapacity,
                new NamedThreadFactory("Boss/threadPool-", true)), 
        		ExecutorUtil.create("TimerExecutor-", timerSize),
        		Executors.newSingleThreadScheduledExecutor()
        		);
        NetSystem.getInstance().setNetConfig(new NetConfig(socketSoRcvbuf, socketSoSndbuf));
        
        RaftServer raftSrv = null;
        TransportServer transportServer = new TransportServer( raftSrv );
        transportServer.start(8080, 4);
        
        System.out.println("start transport server..!!");
		
	}

}
