package com.feeyo.raft.util;

import java.nio.ByteBuffer;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.raft.Errors.RaftException;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

public class ProtobufCodedOutputUtil {

	//
	public static ByteBuffer msgToBuffer(MessageLite msg) throws RaftException {

		ByteBuffer buffer = null;
		try {
			buffer = NetSystem.getInstance().getBufferPool().allocate(msg.getSerializedSize());
			//
			final CodedOutputStream output = CodedOutputStream.newInstance(buffer);
			msg.writeTo(output);
			output.flush();
			//
			return buffer;
			//
		} catch (Throwable e) {
			//
			if (buffer != null)
				NetSystem.getInstance().getBufferPool().recycle(buffer);
			//
			throw new RaftException(e);
		}
	}

}
