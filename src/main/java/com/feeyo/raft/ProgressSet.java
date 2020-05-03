package com.feeyo.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.feeyo.raft.Errors.RaftException;
import com.google.common.collect.Sets;

// `ProgressSet` contains several `Progress`es,
// which could be `Leader`, `Follower` and `Learner`.
//
public class ProgressSet {
	
	private Map<Long, Progress> voters;
	private Map<Long, Progress> learners;
	
	//
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock writeLock = this.readWriteLock.writeLock();
	private final Lock readLock = this.readWriteLock.readLock();
	
	//
	private Set<Long> mut = Sets.newConcurrentHashSet();
	
	public ProgressSet(int voterSize, int learnerSize) {
		this.voters = new ConcurrentHashMap<Long, Progress>(voterSize);
		this.learners = new ConcurrentHashMap<Long, Progress>(learnerSize);
	}
	
	public Map<Long, Progress> voters() {
		//
		readLock.lock();
		try {
			return this.voters;
		} finally {
			readLock.unlock();
		}
	}
	
	public Map<Long, Progress> learners() {
		//
		readLock.lock();
		try {
			return this.learners;
		} finally {
			readLock.unlock();
		}
	}
	
	public List<Long> voterNodes() {
		//
		readLock.lock();
		try {
			List<Long> voterIds = new ArrayList<Long>(voters.size());
			voterIds.addAll(voters.keySet());
			Collections.sort(voterIds);
			return voterIds;
		} finally {
			readLock.unlock();
		}
	}
	
	public List<Long> learnerNodes() {
		//
		readLock.lock();
		try {
			List<Long> learnerIds = new ArrayList<Long>(learners.size());
			learnerIds.addAll(learners.keySet());
			Collections.sort(learnerIds);
			return learnerIds;
		} finally {
			readLock.unlock();
		}
	}
	
	// 归并了 voters & learners 的 id 数据
	public Set<Long> mut(){
		//
		readLock.lock();
		try {
			return mut;
		} finally {
			readLock.unlock();
		}
	}
	
	public Progress get(long id) {
		//
		readLock.lock();
		try {
			Progress pr = this.voters.get(id);
			if ( pr == null )
				pr = this.learners.get(id);
			return pr;
		} finally {
			readLock.unlock();
		}
	}
	
	public void insertVoter(long id, Progress pr) throws RaftException {
		//
		writeLock.lock();
		try {
			if ( this.learners.containsKey(id) ) 
				throw new Errors.RaftException("insert voter " + id + "but already in learners");
			//
			if( this.voters.containsKey(id) ) 
				throw new Errors.RaftException("insert voter " + id + " twice");
			//
			this.voters.put(id, pr);
			this.mut.add(id);
		} finally {
			writeLock.unlock();
		}
	}
	
	public void insertLearner(long id, Progress pr) throws RaftException {	
		//
		writeLock.lock();
		try {
			//
			if ( this.voters.containsKey(id) ) 
				throw new Errors.RaftException("insert learner "+ id +" but already in voters");
			//
			if( this.learners.containsKey(id) ) 
				throw new Errors.RaftException("insert learner " + id + " twice");
			//
			this.learners.put(id, pr);
			this.mut.add(id);
		} finally {
			writeLock.unlock();
		}
	}
	
	public Progress remove(long id)  {	
		//
		writeLock.lock();
		try {
			//
			Progress pr = voters.remove(id);
			if( pr == null ) 
				pr = learners.remove(id);
			this.mut.remove(id);
			return pr;
			//
		} finally {
			writeLock.unlock();
		}
    }
	
	public void promoteLearner(long id) throws RaftException {
		//
		writeLock.lock();
		try {
			Progress pr = this.learners.remove(id);
			if ( pr != null ) {
				 pr.setLearner(false);
				 this.voters.put(id, pr);
				 return;
			}
			throw new Errors.RaftException("promote not exists learner: " + id);
		} finally {
			writeLock.unlock();
		}
	}
}