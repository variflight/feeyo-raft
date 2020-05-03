package com.feeyo.raft.proto.util;

import com.feeyo.net.codec.protobuf.ProtobufEncoder;

import com.feeyo.raft.proto.Btreepb.Element;
import com.feeyo.raft.proto.Btreepb.ElementCmd;
import com.feeyo.raft.proto.Btreepb.Operation;

import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.util.ObjectIdUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroByteStringHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BtreeProposeMsgUtil {

    //
    private static final ProtobufEncoder protobufEncoder = new ProtobufEncoder(true);
    private static final ByteString EMPTY_BYTE_STRING = ByteString.EMPTY;
    //
    // 每个 Entry 限制大小, 防止 Raft 消息过大
    private static final int MAX_ENTRY_SIZE = 128 * 1024;

    //
    // 构造 raft propose 消息

    public static Message createProposeMessage(String key, ByteBuffer value, Operation operation) {

    	Element ele = Element.newBuilder() //
                .setKey(key) //
                .setValue(value == null ? EMPTY_BYTE_STRING : ZeroByteStringHelper.wrap(value)) //
                .build();

    	ElementCmd cmd = ElementCmd.newBuilder() //
                .addEle(ele) //
                .setOp(operation) //
                .build();
        //
        byte[] data = protobufEncoder.encode(cmd);

        Entry entry = Entry.newBuilder() //
                .setEntryType(EntryType.EntryNormal) //
                .setData(ZeroByteStringHelper.wrap(data)) //
                .setCbkey(ObjectIdUtil.getId()).build();

        return Message.newBuilder() //
                .setMsgType(MessageType.MsgPropose) //
                .addEntries(entry) //
                .build();
    }

    // 构造 raft propose 消息
    public static Message createProposeMessage(List<ElementCmd> commands) {

    	List<ByteString> entryDataList = new ArrayList<>();
        ///
        //
        int tempSize = 0;
        List<ElementCmd> tempList = new ArrayList<>();

        //
        for (ElementCmd command : commands) {

        	//
            // 不论是累计的size还是某一条size超过阈值都会触发新的Entry生成
        	int commandSize = command.getSerializedSize();
            if (tempSize + commandSize > MAX_ENTRY_SIZE) {
                tempSize = 0;
                tempList.clear();
                //
                byte[] data = protobufEncoder.encode( tempList );
                entryDataList.add( ZeroByteStringHelper.wrap(data)  );
            }

            tempList.add(command);
            tempSize += commandSize;
        }

        //
        // 剩余的小片一起发送
        if ( !tempList.isEmpty() ) {
            byte[] data = protobufEncoder.encode( tempList );
            entryDataList.add( ZeroByteStringHelper.wrap(data)  );
        }

        //
        // 注入 cbKey
        Message.Builder builder = Message.newBuilder();
        builder.setMsgType(MessageType.MsgPropose);
        //
        int size = entryDataList.size();
        for (int i = 0; i < size; i++) {
            //
            Entry.Builder entryBuilder = Entry.newBuilder();
            entryBuilder.setData( entryDataList.get(i) );
            entryBuilder.setEntryType(EntryType.EntryNormal);
            if ( i == (size - 1) )
            	entryBuilder.setCbkey( ObjectIdUtil.getId() );
            //
            builder.addEntries( entryBuilder.build() );
        }
    	return builder.build();
    }
}