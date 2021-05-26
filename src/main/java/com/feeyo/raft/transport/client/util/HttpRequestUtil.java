package com.feeyo.raft.transport.client.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.feeyo.net.nio.NetSystem;

final class HttpRequestUtil {
    public static final byte[] GET = "GET".getBytes();
    public static final byte[] POST = "POST ".getBytes();
    //
    public static final byte[] HTTP_VERSION = " HTTP/1.1".getBytes();
    public static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    public static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    public static final byte[] CRLF = "\r\n".getBytes();
    //
    private static final byte NEGATIVE = '-';
    private static final byte ZERO = '0';
    //
    // 结果类似于 11 -> "11".getBytes(), 不产生任何其他对象
    private static void writeIntAsBytes(ByteBuffer byteBuffer, int number, int numberByteCount) {
        if (number == 0) {
            byteBuffer.put(ZERO);
        } else if (number > 0) {
            for (int i = 0; i < numberByteCount; i++) {
                int perNumber = number / (int) Math.pow(10, numberByteCount - i - 1);
                number = number % (int) Math.pow(10, numberByteCount - i - 1);
                byteBuffer.put((byte) ((perNumber + 48) & 0xFF));
            }
        } else {
            byteBuffer.put(NEGATIVE);
            number= - number;
            for (int i = 0; i < numberByteCount - 1; i++) {
                int perNumber = number / (int) Math.pow(10, numberByteCount - 2 - i);
                number = number % (int) Math.pow(10, numberByteCount - 2  - i);
                byteBuffer.put((byte) ((perNumber + 48) & 0xFF));
            }
        }
    }

    private static int numberByteCount(int number) {
        if (number == 0) {
            return 1;
        } else if (number > 0) {
            return (int) Math.log10(number) + 1;
        } else {
            return (int) Math.log10(-number) + 2;
        }
    }

    //
    public static ByteBuffer toPostByteBuffer(byte[] pathBytes, ByteBuffer bodyBuffer) {
        //
        // Use direct byte buffer
        ByteBuffer buffer = null;
        try {
            bodyBuffer.flip();
            //
            int contentSize = bodyBuffer.limit();
            int contentSizeByteCount = numberByteCount(contentSize);
            int headSize = POST.length + pathBytes.length + HTTP_VERSION.length + CRLF.length * 3 + CONTENT_LENGTH.length + contentSizeByteCount;
            int totalSize = headSize + contentSize;
            //
            buffer = NetSystem.getInstance().getBufferPool().allocate(totalSize);
            // write header
            buffer.put(POST);
            buffer.put(pathBytes);
            buffer.put(HTTP_VERSION);
            buffer.put(CRLF);
            buffer.put(CONTENT_LENGTH);
            //
            writeIntAsBytes(buffer, contentSize, contentSizeByteCount);
            buffer.put(CRLF);
            buffer.put(CRLF);
            // write body
            buffer.put(bodyBuffer);
            return buffer;
            //
        } catch (BufferOverflowException e) {
            if (buffer != null) NetSystem.getInstance().getBufferPool().recycle(buffer);
            throw e;
        } finally {
            NetSystem.getInstance().getBufferPool().recycle(bodyBuffer);
        }
    }
}