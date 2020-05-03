package com.feeyo.raft.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedByteBufferUtil {
	
	//
    // slice
    public static ByteBuffer slice(MappedByteBuffer buffer, int position, int length) {
    	//
    	ByteBuffer subBuffer;
        int oldPosition = buffer.position();
        int oldLimit = buffer.limit();
        try {
        	 int newLimit = position + length;
             if ( newLimit > buffer.capacity() )
                 throw new IllegalArgumentException("The new limit is " + newLimit + ", but the capacity is " + buffer.capacity());
        	//
	        buffer.limit( newLimit );
	        buffer.position( position );
	        subBuffer = buffer.slice();
	        //
        } finally {
            buffer.limit( oldLimit );
            buffer.position( oldPosition );
        }
        return subBuffer;
    }
	

	// See https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public static void clean(MappedByteBuffer cb) {
        // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
        boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
        try {
            if (isOldJDK) {
                Method cleaner = cb.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(cb));
            } else {
                Class unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (Exception ex) {
                    // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                clean.setAccessible(true);
                Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                Object theUnsafe = theUnsafeField.get(null);
                clean.invoke(theUnsafe, cb);
            }
        } catch (Exception ex) {
        }
        cb = null;
    }

}
