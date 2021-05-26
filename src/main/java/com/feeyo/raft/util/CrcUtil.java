package com.feeyo.raft.util;

import java.util.zip.CRC32;

public class CrcUtil {
	//
	private static final ThreadLocal<CRC32> CRC32_THREAD_LOCAL = new ThreadLocal<CRC32>() {
		protected CRC32 initialValue() {
			return new CRC32();
		}
	};

	public static long crc32(final byte[] array) {
		if (array == null)
			return 0;
		//
		return crc32(array, 0, array.length);
	}

	public static long crc32(final byte[] array, final int offset, final int length) {
		final CRC32 crc32 = CRC32_THREAD_LOCAL.get();
		crc32.update(array, offset, length);
		final long ret = crc32.getValue();
		crc32.reset();
		return ret;
	}

}
