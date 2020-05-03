package com.feeyo.raft.transport.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;

public class PhysicalNode {
	
	private static Logger LOGGER = LoggerFactory.getLogger( PhysicalNode.class );

	private long id;
	private String host;
	private int port;
	private int minCon;
	private int maxCon;
	
	// 连接队列
	private final ConQueue conQueue = new ConQueue();
	
	//
	private final HttpClientConnectionFactory factory;
	
	public PhysicalNode(HttpClientConnectionFactory factory, long id, String host, int port, int minCon, int maxCon) {
		this.factory = factory;
		//
		this.id = id;
		this.host = host;
		this.port = port;
		this.minCon = minCon;
		this.maxCon = maxCon;
	}
	
	public long getId() {
		return id;
	}
	
	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	// 新建连接，异步填充后端连接池
	private void createNewConnection() throws IOException {	
		//
		createNewConnection(new ConCallback() {
			@Override
			public void connectionAcquired(HttpClientConnection conn) {
				conQueue.getCons().add( conn ); 
			}	
			
			public void connectionClose(HttpClientConnection conn, String reason) {
				conQueue.getCons().remove( conn );  
			}
			
			@Override
			public void handleResponse(HttpClientConnection conn, byte[] data) throws IOException {
				//ignore
			}

			@Override
			public void connectionError(HttpClientConnection conn, Exception e) {
				//ignore
			}
		});
	}
	
	private HttpClientConnection createNewConnection(ConCallback callback) throws IOException {
		 int activeCons = this.getActiveCount();// 当前活动连接
         if ( activeCons + 1 > maxCon ) {// 下一个连接数大于最大连接数
         		LOGGER.error("PN={} the max activeConns={} size can not be max than maxConns={}",  id, (activeCons+1), maxCon );
         		//
             	throw new IOException("the max activeConnnections size can not be max than maxconnections");                
         } else {      
         	if ( LOGGER.isDebugEnabled() ) 
         		LOGGER.debug( "no ilde connection, create new connection for {} ", this.id );           
         	//
         	// create connection
         	HttpClientConnection con = factory.make(this, callback);
    		con.setLastTime( TimeUtil.currentTimeMillis() );
    		return con;
         }
	}
	
	public boolean initConnections() {
        int initSize = minCon;
        LOGGER.info("init node ,create connections total " + initSize + " for " + host + ":" + port);
		for (int i = 0; i < initSize; i++) {
			try {
				this.createNewConnection();				
			} catch (Exception e) {
				LOGGER.error(" init connection error.", e);
			}
		}
		
		LOGGER.info("init node finished");
        return true;
    }
	
	public int getActiveCount() {
        return this.conQueue.getActiveCountForNode(this);
    }
	
	public int getIdleCount() {
		return this.conQueue.getCons().size();
	}
	
	

    public HttpClientConnection getConnection()
            throws IOException {
    	//
    	HttpClientConnection con = this.conQueue.takeIdleCon();
        if (con != null) {   	
        	con.setBorrowed( true );
        	con.setLastTime( TimeUtil.currentTimeMillis() ); 
        	//
        	// 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致执行失败
        	return con;
        	
        } else {
        	
        	//
    		final AtomicBoolean isFailure = new AtomicBoolean(false);
    		final CountDownLatch latch = new CountDownLatch(1);
        	//
        	con = createNewConnection(new ConCallback() {
    			@Override
    			public void connectionAcquired(HttpClientConnection conn) {
    				latch.countDown();
    			}	
    			
    			public void connectionClose(HttpClientConnection conn, String reason) {
    				latch.countDown();
    			}
    			
    			@Override
    			public void handleResponse(HttpClientConnection conn, byte[] data) throws IOException {
    				//ignore
    			}
    			
    			@Override
    			public void connectionError(HttpClientConnection conn, Exception e) {
    				isFailure.set(true);
    				latch.countDown();
    			}
    		});
        	
        	//
        	try {
				latch.await();
			} catch (InterruptedException e1) {
				// ignore
			}
        	
        	//
        	if ( isFailure.get() ) {
             	throw new IOException("Failed to create connection!");      
        	}
        	
        	con.setCallback( null );
        	con.setBorrowed( true );
        	con.setLastTime( TimeUtil.currentTimeMillis() ); 
        	return con;
        	
        }
    }
	
	public void releaseConnection(HttpClientConnection c) {
		c.setBorrowed( false );
        c.setCallback( null );
        c.setLastTime( TimeUtil.currentTimeMillis() );     
        
        ConQueue queue = this.conQueue;
        boolean ok = false;
        ok = queue.getCons().offer(c);
        if ( !ok ) {
        	LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
        
        if ( LOGGER.isDebugEnabled() ) {
        	LOGGER.debug("release channel " + c);
        }
    }
	
    public void clearConnections(String reason) {    	
    	this.conQueue.clearConnections(reason, this);
    }

	//
	public ArrayList<HttpClientConnection> getIdleConsToClose(int count) {
		return conQueue.getIdleConsToClose(count);
	}
	
	private int maxConsInOneCheck = 5;
	
	//
	public ArrayList<HttpClientConnection> getNeedHeartbeatCons(long heartbeatTime, long closeTime) {
	
		ArrayList<HttpClientConnection> heartbeatCons = new ArrayList<HttpClientConnection>();
		
		//
		ConcurrentLinkedQueue<HttpClientConnection> checkLis = conQueue.getCons();
		Iterator<HttpClientConnection> checkListItor = checkLis.iterator();
		while (checkListItor.hasNext()) {
			HttpClientConnection con = checkListItor.next();
			if ( con.isClosed() ) {
				checkListItor.remove();
				continue;
			}
			
			// 关闭 闲置过久的 connection
			if (con.getLastTime() < closeTime) {
				if(checkLis.remove(con)) { 
					con.close("heartbeate idle close ");
					continue;
				}
			}
			
			// 提取需要做心跳检测的 connection
			if (con.getLastTime() < heartbeatTime && heartbeatCons.size() < maxConsInOneCheck) {
				// 如果移除失败，说明该连接已经被其他线程使用
				if(checkLis.remove(con)) { 
					con.setBorrowed(true);
					heartbeatCons.add(con);
				}
			} 
		}
		return heartbeatCons;
	}
	
	public void closeByIdleMany(int ildeCloseCount) {
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("too many ilde cons, close some for node  " + this.getId() );
		
		List<HttpClientConnection> readyCloseCons = new ArrayList<HttpClientConnection>( ildeCloseCount);
		readyCloseCons.addAll( this.getIdleConsToClose(ildeCloseCount));

		for (HttpClientConnection idleCon : readyCloseCons) {
			if ( idleCon.isBorrowed() ) {
				LOGGER.warn("find idle con is using " + idleCon);
			}
			idleCon.close("too many idle con");
		}
	}
	
	public void createByIdleLitte(int idleCons, int createCount) {		
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("create connections, because idle connection not enough, cur is {}, minCon is {} for {}", 
					idleCons, minCon, this.getId() );
		//
		for (int i = 0; i < createCount; i++) {			
			int activeCount = this.getActiveCount();
			int idleCount = this.getIdleCount();
			if ( activeCount + idleCount >= maxCon ) {
				break;
			}			
			try {				
				// create new connection
				this.createNewConnection();				
			} catch (IOException e) {
				LOGGER.warn("create connection err ", e);
			}
		}
	}	

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		sb.append("id=").append(id).append(", ");
		sb.append("host=").append(host).append(", ");
		sb.append("port=").append(port);
		sb.append("]");
		return sb.toString();
	}	
}