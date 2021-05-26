package com.feeyo.raft.caller.callback;


import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.RaftStatistics;

/**
 * 
 * Propose & ReadIndex 
 * 
 * @author zhuam
 *
 */
public final class CallbackRegistry {
	
	private static Logger LOGGER = LoggerFactory.getLogger( CallbackRegistry.class );
	//
	// Propose & ReadIndex callback
	private Map<String, Callback> allC = new ConcurrentHashMap<String, Callback>( 10000 );
	//
	// Waiting ReadIndex callback,  appliedIndex >= committedIndex
	private Map<String, ReadIndexCallback> rIndexC = new ConcurrentHashMap<String, ReadIndexCallback>( 5000 );
	//
    private AtomicBoolean running = new AtomicBoolean(false);
    private RaftStatistics raftStatistics;
    
    public CallbackRegistry(RaftStatistics raftStatistics) {
    	this.raftStatistics = raftStatistics;
    }
	//
	public void timeoutCheck() {
		if (!running.compareAndSet(false, true))
			return;
		//
		try {
			long nowMs = TimeUtil.currentTimeMillis();
			Iterator<Entry<String,Callback>> it = allC.entrySet().iterator();
			while ( it.hasNext() ) {
				final Callback cb = it.next().getValue();
				if ( cb == null ) {
					it.remove();
					continue;
				}
				//
				if (cb.isTimeout(nowMs)) {
					cb.onFailed("timeout err!".getBytes());
					LOGGER.warn("timeout, key={}, costMs={}", cb.key, (nowMs - cb.createMs));
					it.remove();
				}
			}				
		} catch(Exception e) {
			LOGGER.warn("timeout err:", e);
		} finally {
			running.set(false);
		}
	}

	public void add(Callback callback) {
		allC.put(callback.key, callback);
	}
	
	public Callback remove(String key) {
		return allC.remove(key);
	}
	//
	// 触发回调
	public void notifyCallbacks(String key, final int action) {
		if (key == null)
			return;
		//
		Callback callback = allC.remove(key);
		if (callback != null) {
        	long elapsed = TimeUtil.since(callback.createMs);
        	raftStatistics.update(RaftStatistics.CALLER_CB, elapsed);
        	//
			if (action == Callback.Action.Commit)
				callback.onCompleted();
			else
				callback.onFailed("action err!".getBytes());
        }
	}
	
	//
	public void notifyCallbacks(String rctx, long index, long appliedIndex) {
		Callback callback = allC.remove(rctx);
		if (callback != null) {
			ReadIndexCallback rIdxCb = (ReadIndexCallback) callback;
			if (index == -1) {
				rIdxCb.onFailed("no leader, dropping".getBytes());
				return;
			}
        	//
			// not applied yet
            if ( index > appliedIndex ) {
            	rIdxCb.index =  index;
            	rIdxCb.recvMs = TimeUtil.currentTimeMillis();
            	rIndexC.put(rIdxCb.key, rIdxCb );
            } else {
            	rIdxCb.onCompleted();
            }
        }
	}
	
	// linearizable read
	public void notifyCallbacks(long appliedIndex) {
		if (rIndexC.isEmpty())
			return;
		//
		long nowMs = TimeUtil.currentTimeMillis();
		Iterator<Entry<String, ReadIndexCallback>> cbIter = rIndexC.entrySet().iterator();
		while ( cbIter.hasNext() ) {
			final ReadIndexCallback readIndexCb = cbIter.next().getValue();
			if ( readIndexCb != null ) {
				// follower applied >= leader committed
				if (readIndexCb.index <= appliedIndex) {		
					cbIter.remove();
					readIndexCb.onCompleted();
					
				} else if (readIndexCb.isTimeout(nowMs)) {
					cbIter.remove();
					readIndexCb.onFailed("timeout err!".getBytes());
				}
			}
		}
	}
	
	public void shutdown() {
		if ( allC != null )
			allC.clear();
		
		if ( rIndexC != null )
			rIndexC.clear();
	}
}