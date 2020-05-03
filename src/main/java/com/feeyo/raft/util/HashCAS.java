package com.feeyo.raft.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class HashCAS {

	private static final int DEFAULT_LOCK_NUM = 100;

	private AtomicBoolean[] locks;
	private int lockSize;

	public HashCAS() {
		this.lockSize = DEFAULT_LOCK_NUM;
		init();
	}

	public HashCAS(int lockSize) {
		if (lockSize <= 0)
			lockSize = DEFAULT_LOCK_NUM;
		this.lockSize = lockSize;
		init();
	}

	private void init() {
		locks = new AtomicBoolean[lockSize];
		for (int i = 0; i < lockSize; i++) {
			locks[i] = new AtomicBoolean( false );
		}
	}

	public boolean compareAndSet(Object obj, boolean expect, boolean update) {
		return this.locks[Math.abs(obj.hashCode()) % lockSize].compareAndSet(expect, update);
	}

	public void set(Object obj, boolean newValue) {
		this.locks[Math.abs(obj.hashCode()) % lockSize].set(newValue);
	}
	
	public boolean get(Object obj) {
		return this.locks[Math.abs(obj.hashCode()) % lockSize].get();
	}
	
	//
	public boolean hasLock() {
		for (int i = 0; i < lockSize; i++) {
			if (  locks[i].get() )
				return true;
		}
		return false;
	}


	public void reset() {
		for (int i = 0; i < lockSize; i++) {
			try {
				locks[i].set(false);
			} catch (Exception ignored) {
			}
		}
	}

}
