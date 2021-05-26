package com.feeyo.raft.util;

public class UUIDUtil {

	private static final boolean USE_JDK_UUID_TO_STRING;

	static {
		int majorVersion = 0;

		try {
			majorVersion = Integer.parseInt(System.getProperty("java.specification.version"));
		} catch (final NumberFormatException ignored) {
			// This will happen for pretty much anything before Java 9, which
			// had a version scheme like "1.8" instead of
			// just "8".
		}

		USE_JDK_UUID_TO_STRING = majorVersion >= 9;
	}

	private static final int UUID_STRING_LENGTH = 36;
	private static final char[] HEX_DIGITS = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	//
	private static ThreadLocal<char[]> uuidCharsGen = new ThreadLocal<char[]>() {
		protected char[] initialValue() {
	        return new char[UUID_STRING_LENGTH];
	    }
	};


	/**
	 * Returns a string representation of the given UUID. The returned string is
	 * formatted as described in {@link UUID#toString()}.
	 */
	public static String uuidToString(final UUID uuid) {

		final long mostSignificantBits = uuid.getMostSignificantBits();
		final long leastSignificantBits = uuid.getLeastSignificantBits();
		//
		final char[] uuidChars = uuidCharsGen.get();
		uuidChars[0] = HEX_DIGITS[(int) ((mostSignificantBits & 0xf000000000000000L) >>> 60)];
		uuidChars[1] = HEX_DIGITS[(int) ((mostSignificantBits & 0x0f00000000000000L) >>> 56)];
		uuidChars[2] = HEX_DIGITS[(int) ((mostSignificantBits & 0x00f0000000000000L) >>> 52)];
		uuidChars[3] = HEX_DIGITS[(int) ((mostSignificantBits & 0x000f000000000000L) >>> 48)];
		uuidChars[4] = HEX_DIGITS[(int) ((mostSignificantBits & 0x0000f00000000000L) >>> 44)];
		uuidChars[5] = HEX_DIGITS[(int) ((mostSignificantBits & 0x00000f0000000000L) >>> 40)];
		uuidChars[6] = HEX_DIGITS[(int) ((mostSignificantBits & 0x000000f000000000L) >>> 36)];
		uuidChars[7] = HEX_DIGITS[(int) ((mostSignificantBits & 0x0000000f00000000L) >>> 32)];
		uuidChars[8] = '-';
		uuidChars[9] = HEX_DIGITS[(int) ((mostSignificantBits & 0x00000000f0000000L) >>> 28)];
		uuidChars[10] = HEX_DIGITS[(int) ((mostSignificantBits & 0x000000000f000000L) >>> 24)];
		uuidChars[11] = HEX_DIGITS[(int) ((mostSignificantBits & 0x0000000000f00000L) >>> 20)];
		uuidChars[12] = HEX_DIGITS[(int) ((mostSignificantBits & 0x00000000000f0000L) >>> 16)];
		uuidChars[13] = '-';
		uuidChars[14] = HEX_DIGITS[(int) ((mostSignificantBits & 0x000000000000f000L) >>> 12)];
		uuidChars[15] = HEX_DIGITS[(int) ((mostSignificantBits & 0x0000000000000f00L) >>> 8)];
		uuidChars[16] = HEX_DIGITS[(int) ((mostSignificantBits & 0x00000000000000f0L) >>> 4)];
		uuidChars[17] = HEX_DIGITS[(int) (mostSignificantBits & 0x000000000000000fL)];
		uuidChars[18] = '-';
		uuidChars[19] = HEX_DIGITS[(int) ((leastSignificantBits & 0xf000000000000000L) >>> 60)];
		uuidChars[20] = HEX_DIGITS[(int) ((leastSignificantBits & 0x0f00000000000000L) >>> 56)];
		uuidChars[21] = HEX_DIGITS[(int) ((leastSignificantBits & 0x00f0000000000000L) >>> 52)];
		uuidChars[22] = HEX_DIGITS[(int) ((leastSignificantBits & 0x000f000000000000L) >>> 48)];
		uuidChars[23] = '-';
		uuidChars[24] = HEX_DIGITS[(int) ((leastSignificantBits & 0x0000f00000000000L) >>> 44)];
		uuidChars[25] = HEX_DIGITS[(int) ((leastSignificantBits & 0x00000f0000000000L) >>> 40)];
		uuidChars[26] = HEX_DIGITS[(int) ((leastSignificantBits & 0x000000f000000000L) >>> 36)];
		uuidChars[27] = HEX_DIGITS[(int) ((leastSignificantBits & 0x0000000f00000000L) >>> 32)];
		uuidChars[28] = HEX_DIGITS[(int) ((leastSignificantBits & 0x00000000f0000000L) >>> 28)];
		uuidChars[29] = HEX_DIGITS[(int) ((leastSignificantBits & 0x000000000f000000L) >>> 24)];
		uuidChars[30] = HEX_DIGITS[(int) ((leastSignificantBits & 0x0000000000f00000L) >>> 20)];
		uuidChars[31] = HEX_DIGITS[(int) ((leastSignificantBits & 0x00000000000f0000L) >>> 16)];
		uuidChars[32] = HEX_DIGITS[(int) ((leastSignificantBits & 0x000000000000f000L) >>> 12)];
		uuidChars[33] = HEX_DIGITS[(int) ((leastSignificantBits & 0x0000000000000f00L) >>> 8)];
		uuidChars[34] = HEX_DIGITS[(int) ((leastSignificantBits & 0x00000000000000f0L) >>> 4)];
		uuidChars[35] = HEX_DIGITS[(int) (leastSignificantBits & 0x000000000000000fL)];
		
		return new String(uuidChars);
	}


	public static String getUuid() {
		//
		if (USE_JDK_UUID_TO_STRING) {
			//
			// @see https://github.com/ibmruntimes/openj9-openjdk-jdk9/blob/openj9/jdk/src/java.base/share/classes/java/util/UUID.java
			//
			// OpenJDK 9 and newer use a fancy native approach to converting
			// UUIDs to strings and we're better off using
			// that if it's available.
			return java.util.UUID.randomUUID().toString();
		}
		return uuidToString( UUID.randomUUID() );
	}

	public static void main(String[] args) {
		System.out.println(getUuid());
	}

}
