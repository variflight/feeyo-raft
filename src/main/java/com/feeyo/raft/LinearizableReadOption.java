package com.feeyo.raft;

// 线性一致读检查
public enum LinearizableReadOption {
	All,
	FollowerRead,
	Ignore;
	
    public static LinearizableReadOption fromString(String value) {
    	
    	if ( value.equalsIgnoreCase("All"))
    		return All;
    	else if ( value.equalsIgnoreCase("FollowerRead"))
    		return FollowerRead;    	
    	else if ( value.equalsIgnoreCase("Ignore"))
    		return Ignore;
    	
    	return All;
    }
}
