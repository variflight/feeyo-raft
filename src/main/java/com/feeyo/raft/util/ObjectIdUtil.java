package com.feeyo.raft.util;

import com.feeyo.net.nio.util.TimeUtil;

public class ObjectIdUtil {
	
	public static String getId() {
		ObjectId objectId = new ObjectId( (int) (TimeUtil.currentTimeMillis() / 1000 ) );
		return objectId.toHexString();
	}

}
