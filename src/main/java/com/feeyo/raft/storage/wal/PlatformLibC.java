package com.feeyo.raft.storage.wal;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;


/**
 * 锁住内存是为了防止这段内存被操作系统swap掉
 */
public class PlatformLibC {
	
    private static final boolean isWindows = Platform.isWindows();
    private static CLibrary cLibrary = CLibrary.INSTANCE;
    
    public static final int MADV_WILLNEED = 3;
    
    
    /**
   	 * 系统调用 mlock 家族允许程序在物理内存上锁住它的部分或全部地址空间。 这将阻止Linux 将这个内存页调度到交换空间（swap
   	 * space），即使 该程序已有一段时间没有访问这段空间。
   	 * <p>
   	 * 需注意的是，仅分配内存并调用 mlock
   	 * 并不会为调用进程锁定这些内存，因为对应的分页可能是写时复制（copy-on-write）的。因此，你应该在每个页面中写入一个假的值：
   	 * <pre>{@code 
   	 * size_t i; 
   	 * size_t page_size = getpagesize (); 
   	 * for (i = 0; i <alloc_size; i += page_size) 
   	 * 	   memory[i] = 0; 
   	 * } </pre>
   	 * 这样针对每个内存分页的写入操作会强制 Linux
   	 * 为当前进程分配一个独立、私有的内存页。
   	 * </p>
   	 * 
   	 * @param address
   	 * @param size
   	 * @return
   	 */
    public static int mlock(Pointer address, NativeLong size) {
        if (isWindows) {
            return cLibrary.VirtualLock(address, size);
        } else {
            return cLibrary.mlock(address, size);
        }
    }
    
    /**
   	 * 解除 mlock 锁定
   	 * @param address   内存地址
   	 * @param size   内存大小
   	 * @return
   	 */
    public static int munlock(Pointer address, NativeLong size) {
        if (isWindows) {
            return cLibrary.VirtualUnlock(address, size);
        } else {
            return cLibrary.munlock(address, size);
        }
    }
    
    /**
	 * 
	 * madvise() 函数建议内核,在从 addr 指定的地址开始,长度等于 len 参数值的范围内,该区域的用户虚拟内存应遵循特定的使用模式
	 * 
	 * @param address 内存起始位置
	 * @param size 内存大小
	 * @param advice 使用模式
	 * @return
	 */
    public static int madvise(Pointer address, NativeLong size, int advice) {
        if (isWindows) {
            return 0; // no implementation in Windows 7 and earlier
        } else {
            return cLibrary.madvise(address, size, advice);
        }
    }
    
    //
    interface CLibrary extends Library {
    	//
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary(Platform.isWindows() ? "kernel32" : "c", CLibrary.class);

        /**
         * call for windows
         * @param address A pointer to the base address of the region of pages to be unlocked
         * @param size The size of the region being unlocked, in bytes. 
         * <strong>NOTE:</strong> a 2-byte range straddling a page boundary causes both pages to be unlocked.
         * @return If the function succeeds, the return value is nonzero otherwise the return value is zero.
         */
        int VirtualLock(Pointer address, NativeLong size);
        
        /**
         * call for windows
         * @param address A pointer to the base address of the region of pages to be unlocked.
         * @param size The size of the region being unlocked, in bytes.
         * <strong>NOTE:</strong> a 2-byte range straddling a page boundary causes both pages to be unlocked.
         * @return If the function succeeds, the return value is nonzero otherwise the return value is zero.
         */
        int VirtualUnlock(Pointer address, NativeLong size);
        
        // call for linux
        int mlock(Pointer address, NativeLong size);
        int munlock(Pointer address, NativeLong size);
        int madvise(Pointer address, NativeLong size, int advice);
    }
}
