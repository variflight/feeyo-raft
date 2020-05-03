package com.feeyo.raft.proto.util;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.KeyValue;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.MessageType;

import com.feeyo.raft.proto.Newdbpb.ColumnFamilyHandle;
import com.feeyo.raft.proto.Newdbpb.Operation;
import com.feeyo.raft.proto.Newdbpb.ProposeCmd;

import com.feeyo.raft.util.ObjectIdUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroByteStringHelper;

public class NewdbProposeMsgUtil {
	
	//
	private static final ProtobufEncoder protobufEncoder = new ProtobufEncoder(true);
	//
	// 每个 Entry 限制大小, 防止 Raft 消息过大
	private static final int MAX_ENTRY_SIZE = 128 * 1024;	

	//
	// 构造 raft propose 消息
	//
	public static Message createProposeMessage(ColumnFamilyHandle cfh, byte[] key, byte[] value,
			Operation operation) {
		return createProposeMessage(cfh, key, key.length, value, value.length, operation);
	}

	//
	// 构造 raft propose 消息
	//
	public static Message createProposeMessage(ColumnFamilyHandle cfh, byte[] key, int keyLength, byte[] value, //
			int valueLength, Operation operation) {

        KeyValue keyValue = KeyValue.newBuilder() //
                .setKey( ZeroByteStringHelper.wrap(key, 0, keyLength) ) //
                .setValue( ZeroByteStringHelper.wrap(value, 0, valueLength) ) //
                .build();

        ProposeCmd cmd = ProposeCmd.newBuilder() //
                .addKv(keyValue) //
                .setOp(operation) //
                .setCfh(cfh) //
                .build();
        //
        byte[] data = protobufEncoder.encode(cmd);

        Entry entry = Entry.newBuilder() //
                .setEntryType(EntryType.EntryNormal) //
                .setData(ZeroByteStringHelper.wrap(data)) //
                .setCbkey( ObjectIdUtil.getId() )
                .build();

        return Message.newBuilder() //
                .setMsgType(MessageType.MsgPropose) //
                //.setFrom(1000) //
                .addEntries( entry ) //
                .build();
	}

    //
	// 构造 raft propose 消息
	//
	public static Message createProposeMessage(List<ProposeCmd> commands) {
		
		List<ByteString> entryDataList = new ArrayList<>();
        ///
        //
	    int tempSize = 0;
        List<ProposeCmd> tempList = new ArrayList<>();

        // 以下场景会发包
        // 1. 达到list的末尾(即使没有null记录, 一般适用于单条记录或者批量的最后一批)
        // 2. 大小超过 MAX_ENTRY_SIZE 且遇到一个 null 记录
        //
        boolean recordFinish = false;
        //
        for (ProposeCmd command : commands) {
        	//
            // 发现 null 标志则修改 recordFinish
        	if (command == null) {
                recordFinish = true;
                continue;
            }
        	//
            // 不论是累计的size还是某一条size超过阈值都会触发新的Entry生成
        	int commandSize = command.getSerializedSize();
            if (recordFinish && tempSize + commandSize > MAX_ENTRY_SIZE) {
                tempSize = 0;
                tempList.clear();
                //
                byte[] data = protobufEncoder.encode( tempList );
                entryDataList.add( ZeroByteStringHelper.wrap(data)  );
            }

            recordFinish = false;
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
        for(int i = 0; i < size; i++) {
            //
            Entry.Builder entryBuilder = Entry.newBuilder();
            entryBuilder.setEntryType(EntryType.EntryNormal);
            entryBuilder.setData( entryDataList.get(i) );
            if ( i == (size - 1) ) 
            	entryBuilder.setCbkey( ObjectIdUtil.getId() );
            //
            builder.addEntries( entryBuilder.build() );
        }
		return builder.build();
	}

}
