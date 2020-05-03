package com.feeyo.raft.transport.client.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.util.ByteBufferUtil;

final class HttpRequestUtil {

    //
    public static final byte[] GET = "GET".getBytes();
    public static final byte[] POST = "POST ".getBytes();
    //
    public static final byte[] HTTP_VERSION = " HTTP/1.1".getBytes();
    public static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    public static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    //
    public static final byte[] CRLF = "\r\n".getBytes();

    //
    public static ByteBuffer toPostByteBuffer(byte[] pathBytes, ByteBuffer bodyBuffer) {
        //
        // Use direct byte buffer
        ByteBuffer buffer = null;
        try {
            //
            bodyBuffer.flip();
            //
            int contentSize = bodyBuffer.limit();
            int contentSizeByteCount = ByteBufferUtil.numberByteCount(contentSize);
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
            ByteBufferUtil.writeIntAsBytes(buffer, contentSize, contentSizeByteCount);
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