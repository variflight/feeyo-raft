package com.feeyo.raft;

public enum ReadOnlyOption {
	//
	// Safe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    Safe,
    
    // LeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    LeaseBased;
    
    public static ReadOnlyOption fromString(String value) {
    	if ( value.equalsIgnoreCase("Safe"))
    		return Safe;
    	else if ( value.equalsIgnoreCase("LeaseBased"))
    		return LeaseBased;
    	return Safe;
    }
}
