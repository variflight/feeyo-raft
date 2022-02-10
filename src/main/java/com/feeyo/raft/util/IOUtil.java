package com.feeyo.raft.util;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);
	
    /**
     * Close a closeable.
     */
    public static int closeQuietly(final Closeable closeable) {
    	//
        if (closeable == null) 
            return 0;
        //   
        try {
            closeable.close();
            return 0;
        } catch (final IOException e) {
        	LOGGER.error("Fail to close", e);
            return -1;
        }
    }

}
