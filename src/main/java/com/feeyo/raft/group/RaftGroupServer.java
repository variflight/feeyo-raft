package com.feeyo.raft.group;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.Config;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.caller.RaftCaller;
import com.feeyo.raft.caller.RaftGroupCallerImpl;
import com.feeyo.raft.config.RaftGroupConfig;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.group.proto.Raftgrouppb.Region;
import com.feeyo.raft.transport.AbstractTransportClient;
import com.feeyo.raft.transport.TransportMultiClient;
import com.feeyo.raft.transport.TransportServer;
import com.feeyo.raft.util.NamedThreadFactory;
import com.feeyo.raft.util.StandardThreadExecutor;
import com.google.common.base.Joiner;

/**
 * 	Multi-raft 
 * 
 *  1、多个 Raft Group 相互独立，各自负责各自的选举和日志 commit
 *  2、网络连接复用
 *  3、核心调度资源公用
 *  
 *  @see https://zhuanlan.zhihu.com/p/33047950
 *  @see https://segmentfault.com/a/1190000008007027
 *  @see https://zhuanlan.zhihu.com/p/24809131?refer=newsql
 * 
 * @author zhuam
 *
 */
public class RaftGroupServer {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftGroupServer.class );
	//
	private Joiner joiner = Joiner.on("-");
	//
	private Map<Long, RaftGroup> rfGroups = new ConcurrentHashMap<Long, RaftGroup>();
	protected RaftGroupConfig raftGroupConfig;
	protected PeerSet peerSet;
	protected RaftGroupStateMachineAdapter stateMachine;
	//
	// Loop Thread & TheadPool
	private AtomicBoolean running = new AtomicBoolean( false );
	private ThreadFactory threadFactory = new NamedThreadFactory("mrf/loop-", true);
	private Thread coreLoopThread;
	private StandardThreadExecutor loopTpExecutor;
 	private StandardThreadExecutor dispatchTpExecutor;
 	private StandardThreadExecutor notifyTpExecutor;
	//
	// Network I/O
 	private TransportServer transportServer;
 	private AbstractTransportClient<RaftGroupMessage> transportClient;
	//
	private RaftCaller<RaftGroupMessage> raftCaller;
	//
	public RaftGroupServer(RaftGroupConfig raftGroupConfig, RaftGroupStateMachineAdapter stateMachine) {
		this.raftGroupConfig = raftGroupConfig;
		this.peerSet = raftGroupConfig.getPeerSet();
		this.stateMachine = stateMachine;
		//
		//  Build groups
		final Config cfg = raftGroupConfig.getCfg();
		for(Region region: raftGroupConfig.getRegions())
			rfGroups.put(region.getId(), new RaftGroup(this, region, cfg) );
		//
		// Asynchronous thread pool  dispatch & notify
		int coreThreads = Math.max(raftGroupConfig.getTpCoreThreads() / 3, 4);
		int maxThreads = Math.max(raftGroupConfig.getTpMaxThreads(), coreThreads);
		int queueCapacity = Math.min(raftGroupConfig.getTpQueueCapacity(), 1500);
		//
		this.loopTpExecutor = new StandardThreadExecutor(coreThreads, maxThreads, Math.max(queueCapacity, rfGroups.size()), new NamedThreadFactory("mrf/loop-"));
		this.loopTpExecutor.prestartAllCoreThreads();
		//
		this.dispatchTpExecutor = new StandardThreadExecutor(coreThreads, maxThreads, queueCapacity, new NamedThreadFactory("mrf/dispatch-"));
		this.dispatchTpExecutor.prestartAllCoreThreads();
		//
		this.notifyTpExecutor = new StandardThreadExecutor(coreThreads, maxThreads, queueCapacity, new NamedThreadFactory("mrf/notify-"));
		this.notifyTpExecutor.prestartAllCoreThreads();
		//
		this.raftCaller = new RaftGroupCallerImpl(this);
	}
	
	//
	public void start(int reactorSize) throws Throwable {
		if ( !running.compareAndSet(false , true) )
			return;
		
		LOGGER.info("------------------------------ Multi-raft ------------------------------");
		try {
			//
			// 1、Start all raft groups
			for(RaftGroup group: rfGroups.values())
				group.start();
			//
			// 2、Create transport for raft
			this.transportServer = new TransportServer(RaftGroupServer.this);
			this.transportServer.start(raftGroupConfig.getLocal().getPort(), reactorSize);
			this.transportClient = new TransportMultiClient();
			this.transportClient.start(peerSet );
			//
			// 3、Start core & schedule thread
			this.startCoreLoopThread();
			this.startScheduleThread();
			System.out.println("raft-group startup, port=" + raftGroupConfig.getLocal().getPort() );
			//
		} catch (Throwable e) {
			LOGGER.error("raft-group init err: ",  e);
			throw e;
		} 
	}
	//
	public boolean isRunning() {
		return running.get();
	}
	
	//
	// 核心循环处理线程
	private void startCoreLoopThread() throws Exception {
		//
		// event loop on raft state machine updates
		this.coreLoopThread = threadFactory.newThread(new Runnable() {
			@Override
			public void run() {
				final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>( null );
				while (running.get()) {
					try {
						int total = rfGroups.values().size();
						int parties = 0;
						final CountDownLatch latch = new CountDownLatch( total );
						//
						// Parallel between groups
						for (final RaftGroup group : rfGroups.values()) {
							parties++;
							//
							loopTpExecutor.execute(new Runnable() {
								@Override
								public void run() {
									try {
										group.passiveLoop();
									} catch (Throwable e) {
										errorRef.set(e);
									} finally {
										latch.countDown();
									}
								}
							});
						}
						//
						// fix
						for(int i= 0; i < Math.abs(total - parties); i++) 
							latch.countDown();
						latch.await();
						//
						if (errorRef.get() != null)
							throw errorRef.get();
						//
					} catch (Throwable e) {
						// 忽略 java.util.concurrent.RejectedExecutionException
						if ( e instanceof RejectedExecutionException )
							return;
						//
						running.set( false );
						LOGGER.error("raft-group coreLoopThread err: ", e);
					}
				}
			}
		});
		this.coreLoopThread.start();
	}
	
	private void startScheduleThread() {
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				// 客户端连接的心跳检测
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						if (transportClient != null)
							transportClient.check();
					}
				});
			}
		}, 15L, 10L, TimeUnit.SECONDS);	
		
		//
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				// Callback 超时检测
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						//
						for (RaftGroup group : rfGroups.values()) 
							group.getCallbackRegistry().timeoutCheck();
					}
				});
			}
		}, 30L, 20L, TimeUnit.MILLISECONDS);	
		
		//
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				// 过期快照文件的清理
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						for (RaftGroup group : rfGroups.values()) 
							group.getSnapshotter().gc();
					}
				});
			}
		}, 12, 12, TimeUnit.MINUTES);
		
		//
		NetSystem.getInstance().getTimerSchedExecutor().scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				// 定时输出 Metric
				NetSystem.getInstance().getTimerExecutor().execute(new Runnable() {
					@Override
					public void run() {
						for (RaftGroup group : rfGroups.values()) {
							String ident = joiner.join("raft-group", group.getId(), group.getRegionId());
							group.getMetrics().print(ident);
						}
					}
				});
			}	
		}, 5, 5, TimeUnit.MINUTES);
	}
	
	//
	public void stop() {
		if ( !running.compareAndSet(true , false) ) 
			return;
		// 
		for(RaftGroup group: rfGroups.values()) 
			group.stop();	
		//
		// 依赖 running 标记实现关闭 coreLoopThread
		//
		if ( this.loopTpExecutor != null )
			this.loopTpExecutor.shutdown();
		
		if ( this.dispatchTpExecutor != null )
			this.dispatchTpExecutor.shutdownNow();
		
		if ( this.notifyTpExecutor != null )
			this.notifyTpExecutor.shutdownNow();
		//
		if ( this.transportClient != null)
			this.transportClient.stop();
		
		if( this.transportServer != null ) 
			this.transportServer.stop();
	}
	
	//
	// ------------------------------- executor ----------------------------------
	// 
	public StandardThreadExecutor getDispatchTpExecutor() {
		return dispatchTpExecutor;
	}

	public StandardThreadExecutor getNotifyTpExecutor() {
		return notifyTpExecutor;
	}

	public TransportServer getTransportServer() {
		return transportServer;
	}

	public AbstractTransportClient<RaftGroupMessage> getTransportClient() {
		return transportClient;
	}
	
	public RaftGroupStateMachineAdapter getStateMachine() {
		return stateMachine;
	}

	//
	// ------------------------------- Adjust group with region changes ------------------------------------
	public void addOrUpdateGroup(Region region, Config config) throws Throwable  {
		long regionId = region.getId();
		RaftGroup group = rfGroups.remove(regionId);
		if (group != null ) 
			group.stop();
		//
		group = new RaftGroup(this, region, config);
		group.start();
		rfGroups.put(regionId, group);
	}
	
	public void removeGroupByRegionId(long regionId) {
		RaftGroup group = rfGroups.remove(regionId);
		if (group != null ) 
			group.stop();
	}
	
	public RaftGroup findGroupByRegionId(long regionId) {
		return rfGroups.get(regionId);
	}
	
	//
	public long getId() {
		return raftGroupConfig.getLocal().getId();
	}
	//
	public PeerSet getPeerSet() {
		return peerSet;
	}
	//
	public RaftCaller<RaftGroupMessage> getRaftCaller() {
		return raftCaller;
	}
}