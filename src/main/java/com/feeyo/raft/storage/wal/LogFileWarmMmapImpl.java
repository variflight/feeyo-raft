package com.feeyo.raft.storage.wal;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

public class LogFileWarmMmapImpl extends LogFileMmapImpl {
	
	private static Logger LOGGER = LoggerFactory.getLogger( LogFileWarmMmapImpl.class );
	//
	private static int OS_PAGE_SIZE = 1024 * 4;
	
	public LogFileWarmMmapImpl(String path, long firstLogIndex, long lastLogIndex, int fileSize, boolean isWritable)
			throws IOException {
		super(path, firstLogIndex, lastLogIndex, fileSize, isWritable);
	}
	//
	//
	// 预热
	@Override
	public void warmup() {
		//
		isWarmed = true;
		//
		long beginTime = TimeUtil.currentTimeMillis();
		java.nio.ByteBuffer byteBuffer = mappedByteBuffer.slice();
		//
        for(int i = 0, j = 0; i < this.fileSize; i += OS_PAGE_SIZE, j++) {
        	byteBuffer.put(i, (byte) 0);
            // prevent gc
            if (j % 1000 == 0) {
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                	LOGGER.error("Interrupted", e);
                }
            }
        }
        //
        LOGGER.info("warm-up {} done. elapsed={}", fileName, TimeUtil.since(beginTime));
        this.mlock();
	}
	
	@Override
	protected void deleteWarmUp() {
		munlock();
	}
	
	//
	// 过mlock可以将进程使用的部分或者全部的地址空间锁定在物理内存中，防止其被交换到swap空间
    // 对时间敏感的应用会希望全部使用物理内存，提高数据访问和操作的效率
	@SuppressWarnings("restriction")
	public void mlock() {
		final long beginTime = TimeUtil.currentTimeMillis();
		final long address = ((sun.nio.ch.DirectBuffer) (mappedByteBuffer)).address();
		Pointer pointer = new Pointer(address);
		{
			int ret = PlatformLibC.mlock(pointer, new NativeLong(fileSize));
			LOGGER.info("mlock {} {} {} ret={} elapsed={}", address, fileName, fileSize, ret, TimeUtil.since(beginTime) );
		}

		{
			// 当用户态应用使用MADV_WILLNEED命令执行 madvise() 系统调用时，它会通知内核，某个文件内存映射区域中的给定范围的文件页不久将要被访问
			int ret = PlatformLibC.madvise(pointer, new NativeLong(fileSize), PlatformLibC.MADV_WILLNEED);
			LOGGER.info("madvise {} {} {} ret={} elapsed={}", address, fileName, fileSize, ret, TimeUtil.since(beginTime));
		}
	}

	@SuppressWarnings("restriction")
	public void munlock() {
		final long beginTime = TimeUtil.currentTimeMillis();
		final long address = ((sun.nio.ch.DirectBuffer) (mappedByteBuffer)).address();
		Pointer pointer = new Pointer(address);
		int ret = PlatformLibC.munlock(pointer, new NativeLong(fileSize));
		LOGGER.info("munlock {} {} {} ret={} elapsed={}", address, fileName, fileSize, ret, TimeUtil.since(beginTime));
	}

}
