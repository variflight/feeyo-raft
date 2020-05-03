package com.feeyo.raft.transport.client.util;

import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.net.codec.protobuf.ProtobufEncoderV3;
import com.feeyo.raft.proto.Raftpb;

public final class RaftMessageUtil {
    //
    private static ProtobufEncoderV3 protobufEncoder = new ProtobufEncoderV3(true);
    private static final byte[] PATH = "/raft/message".getBytes();

    //
    public static ByteBuffer toByteBuffer(List<Raftpb.Message> messages) throws Throwable {
        ByteBuffer bodyBuffer = protobufEncoder.encode(messages);
        return HttpRequestUtil.toPostByteBuffer(PATH, bodyBuffer);
    }

    //
    public static ByteBuffer toByteBuffer(Raftpb.Message message) throws Throwable {
        ByteBuffer bodyBuffer = protobufEncoder.encode(message);
        return HttpRequestUtil.toPostByteBuffer(PATH, bodyBuffer);
    }
}