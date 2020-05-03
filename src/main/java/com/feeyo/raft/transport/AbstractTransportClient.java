package com.feeyo.raft.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.nio.NIOConnector;
import com.feeyo.net.nio.NIOReactorPool;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.PeerSet;
import com.feeyo.raft.transport.client.AsyncCallback;
import com.feeyo.raft.transport.client.AsyncHttpResponseHandler;
import com.feeyo.raft.transport.client.HttpClientConnection;
import com.feeyo.raft.transport.client.PhysicalNode;
import com.feeyo.raft.transport.client.pool.ClientNodePool;

/**
 * 抽象传输能力，供客户端使用
 * 
 * @author zhuam
 *
 * @param <M> 
 */
public abstract class AbstractTransportClient<M> {
	//
	protected static Logger LOGGER = LoggerFactory.getLogger( AbstractTransportClient.class );
	//
	protected NIOReactorPool nioReactorPool = null;
	protected NIOConnector nioConnector = null;
	//
	protected ClientNodePool clientPool = null;
	
	//
	public AbstractTransportClient() {
		// ignored
	}
	 
	//
	public void start(PeerSet peerSet) throws IOException {
		this.nioReactorPool = new NIOReactorPool("rf/tc/reactor", 2);
		this.nioConnector = new NIOConnector("rf/tc/connector", nioReactorPool);
		this.nioConnector.start();
		
		this.clientPool = new ClientNodePool(nioConnector);
		this.clientPool.start( peerSet );
	}

	public void stop() {
		if( this.nioConnector != null) {
			this.nioConnector.interrupt();
			this.nioConnector = null;
		}
		//
		if ( clientPool != null ) {
			clientPool.stop();
		}
	}
	
	// peerSet 发生变化后，此处需要 reload
	public void reload(PeerSet peerSet) {
		//
		ClientNodePool oldPool = clientPool;
		//
		this.clientPool = new ClientNodePool(nioConnector);
		this.clientPool.start( peerSet );
		//
		if ( oldPool != null ) {
			oldPool.stop();
			oldPool = null;
		}	
	}
	
	// 检测连接
	public void check() {
		try {
			if ( clientPool != null ) 
				clientPool.check();
		} catch(Throwable e) {
			LOGGER.warn("transport client check err:", e);
		}
	}
	
	//
	// 同步
	public abstract boolean syncPost(M message);
	public abstract boolean syncBatchPost(List<M> messages);
	
	//
	// 异步
	public abstract void asyncPost(M message);
	public abstract void asyncPost(M message, AsyncHttpResponseHandler handler);
	public abstract void asyncBatchPost(List<M> messages);
	
	//
	// 流水线/pipeline 
	public abstract void pipeliningPost(M message);
	public abstract void pipeliningBatchPost(List<M> messages);
	
	//
	//
	public boolean syncSend(long id, ByteBuffer buffer) {
		//
		final AtomicBoolean isFailure = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(1);
		//
		asyncSend(id, buffer, new AsyncHttpResponseHandler() {
			@Override
			public void completed(HttpResponse response) {
				if (response == null || response.getStatusCode() != 200) 
					isFailure.set(true);
				latch.countDown();
			}
			@Override
			public void failed(String reason) {
				isFailure.set(true);
				latch.countDown();
			}
		});
        
        try {
        	latch.await();
        } catch (InterruptedException e) { /* ignore*/ }
        //
        return isFailure.get();
	}
	
	
	public boolean asyncSend(long id, ByteBuffer buffer, AsyncHttpResponseHandler handler) {
		try {
			PhysicalNode physicalNode = clientPool.getPhysicalNode( id );
			if ( physicalNode == null ) {
				NetSystem.getInstance().getBufferPool().recycle( buffer );
				return false;
			}
			//
			HttpClientConnection connection = physicalNode.getConnection();
			connection.setCallback( new AsyncCallback( handler ) );
			connection.write( buffer );
			return true;
		} catch(IOException e) {
			NetSystem.getInstance().getBufferPool().recycle( buffer );
			//
			if ( handler != null )
				handler.failed( ExceptionUtils.getStackTrace(e));
			//
			return false;
		}
	}
	
	//
	// TODO: 支持 pipeliling
	//
	private ConcurrentHashMap<Long, HttpClientConnection> conns = new ConcurrentHashMap<>();
	private Object _getLock = new Object();
	
	protected boolean pipeliningSend(long id, ByteBuffer buffer) {
		try {
			//
			PhysicalNode physicalNode = clientPool.getPhysicalNode( id );
			if ( physicalNode == null ) {
				NetSystem.getInstance().getBufferPool().recycle( buffer );
				return false;
			}

			//
			HttpClientConnection connection = conns.get( id );
			if ( connection != null && !connection.isClosed() ) {
				connection.setCallback( null );
				connection.write( buffer );
				return true;
			} 
			
			//
			synchronized ( _getLock ) {
				// new connection
				connection = conns.get( id );
				if ( connection == null || connection.isClosed() )  {
					try {
						if ( connection != null ) {
							if ( LOGGER.isDebugEnabled() )
								LOGGER.debug("close pipe {}", connection.toString());
							//
							connection.close(null);
							conns.remove(id);
						}
						//
						connection = physicalNode.getConnection();
						connection.setCallback( null );
						connection.write( buffer );
						//
						conns.put(id, connection);
						//
						if ( LOGGER.isDebugEnabled() )
							LOGGER.debug("new pipe {}", connection.toString());
						
					} catch(IOException ioe) {
						NetSystem.getInstance().getBufferPool().recycle( buffer );
						return false;
					}
				} else {
					connection.write( buffer );
				}
			}
			return true;
			//
		} catch(Throwable e) {
			LOGGER.warn("transport client err:", e);
			return false;
		}
	}
}