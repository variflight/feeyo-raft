package com.feeyo.raft.util;

import java.util.concurrent.locks.ReentrantLock;

//
public class HashLock {
	
    private static final int DEFAULT_LOCK_NUM = 100;

    private ReentrantLock[] locks;
    private int lockSize;

    public HashLock() {
        this.lockSize = DEFAULT_LOCK_NUM;
        init();
    }

    public HashLock(int lockSize) {
        if(lockSize <= 0)
            lockSize = DEFAULT_LOCK_NUM;
        this.lockSize = lockSize;
        init();
    }

    private void init() {
        locks = new ReentrantLock[lockSize];
        for(int i = 0; i < lockSize; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public void lock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].lock();
    }

    public void unlock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].unlock();
    }

    

    /**
     * This method will unlock all locks. Only for test convenience.
     */
    public void reset() {
        for(int i = 0; i < lockSize; i++) {
            try {
                locks[i].unlock();
            } catch (Exception ignored) {
            }
        }
    }
}