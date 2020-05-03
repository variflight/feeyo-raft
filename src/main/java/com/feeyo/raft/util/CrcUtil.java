package com.feeyo.raft.util;

import java.nio.ByteBuffer;
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
		final CRC32 crc64 = CRC32_THREAD_LOCAL.get();
		crc64.update(array, offset, length);
		final long ret = crc64.getValue();
		crc64.reset();
		return ret;
	}

	public static long crc64(final ByteBuffer buf) {
		final int pos = buf.position();
		final int rem = buf.remaining();
		if (rem <= 0) 
			return 0;
		//
		// Currently we have not used DirectByteBuffer yet.
		if (buf.hasArray()) {
			return crc32(buf.array(), pos + buf.arrayOffset(), rem);
		}
		//
		final byte[] b = new byte[rem];
		buf.mark();
		buf.get(b);
		buf.reset();
		return crc32(b);
	}

}
