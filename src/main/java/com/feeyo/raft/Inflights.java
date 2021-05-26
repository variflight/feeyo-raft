package com.feeyo.raft;

import com.feeyo.raft.Errors.RaftException;

/**
 * @see https://github.com/tikv/raft-rs/blob/master/src/tracker/inflights.rs
 * @see https://github.com/etcd-io/etcd/blob/main/raft/tracker/inflights.go
 * 
 * MsgAppend 流量控制,类似TCP滑动窗口 (由其中包含的最大索引表示)
 * 
 * Inflights limits the number of MsgApp (represented by the largest index contained within) 
 * sent MsgAppend to followers but not yet acknowledged by them. Callers use Full() to check whether more messages can be sent, 
 * call Add() whenever  they are sending a new append, and release "quota" via freeTo() whenever an ack is received
 *
 *  @ Raft.maybeSendAppend(Inflights.add) & StepLeader.MsgAppendResponse(Inflights.freeTo)
 */
public class Inflights {
	// the starting index in the buffer
    private volatile int start;	 
    // number of inflights in the buffer
    private volatile int count;	 
    
    // the size of the buffer
    private int size; 
    
    // buffer contains the index of the last entry 
    // inside one message.
    private volatile long[] buffer = new long[0]; 

    public Inflights(int size) {
        this.size = size;
    }
    //
	// Add notifies the Inflights that a new message with the given index is being
	// dispatched. Full() must be called prior to Add() to verify that there is room
	// for one more message, and consecutive calls to add Add() must provide a
	// monotonic sequence of indexes.
    public synchronized void add(long inflight) throws RaftException {
        if (full()) 
            throw new Errors.RaftException("cannot add into a full inflights");
        //
        int next = start + count; // 获取新增消息的下标
        if (next >= size) { 
            next -= size; 
        }
        if (next >= buffer.length) {
        	grow();
        }
        buffer[next] = inflight; // 在next位置记录消息中最后一条Entry记录的索引值
        count++;				 // 递增count字段
    }

    //
    // grow the inflight buffer by doubling up to inflights.size. We grow on demand
    // instead of preallocating to inflights.size to handle systems which have
    // thousands of Raft groups per process.
    private void grow() {
        int newSize = buffer.length * 2;
        if (newSize == 0) {
            newSize = 1;
        } else if (newSize > size) {
            newSize = size;
        }
        //
        long[] newBuffer = new long[newSize];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        this.buffer = newBuffer;
    }
    //
    // free_to frees the inflights smaller or equal to the given `to` flight.
    public synchronized void freeTo(long to) {
    	if (count == 0 || to < buffer[start]) {
    		// out of the left side of the window
			return;
    	}
    	//
		int idx = start;
		int i = 0;
		for (i = 0; i < count; i++) {
			if (to < buffer[idx]) {	// found the first large inflight
				break;
			}
			
			// increase index and maybe rotate
			idx++;	
			if (idx >= size) {
				idx -= size;
			}
		}
		//
		// free i inflights and set new start index
		count -= i;
		start = idx;
		if (count == 0) {
			// inflights is empty, reset the start index so that we don't grow the 
			// buffer unnecessarily.
			start = 0; 
		}
    }

    // freeFirstOne releases the first inflight. This is a no-op if nothing is
    // inflight.
	public synchronized void freeFirstOne() {
		freeTo(buffer[start]);
	}
	
    //
    // full returns true if no more messages can be sent at the moment
    public boolean full() {
        return count == size;
    }
    
    //
    // count returns the number of inflight messages.
    public int count() {
    	return count;
    }
    
    //
    // resets frees all inflights.
    public void reset() {
        count = 0;
        start = 0;
    }
}