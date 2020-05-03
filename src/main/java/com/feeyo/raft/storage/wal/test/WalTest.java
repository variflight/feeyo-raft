package com.feeyo.raft.storage.wal.test;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.storage.wal.AbstractLogFile;
import com.feeyo.raft.storage.wal.proto.Walpb;
import com.feeyo.raft.util.Pair;
import com.feeyo.raft.util.Util;

public class WalTest {
	
	//
	public static void main(String[] args) throws Throwable {
		
		String walDir = "/Users/zhuam/Downloads/xx3/wal";
		
		long snapshotIndex = -1;
		
		Walpb.Snapshot start = null;
		List<Entry> entries = new ArrayList<Entry>();
		Raftpb.HardState state = null;
		
		List<String> fileNames = Util.getSortedFileNamesInDirectory(walDir);
		for (int i = 0; i < fileNames.size(); i++) {

			String fileName = fileNames.get(i);
			Pair<Long, Long> pair = AbstractLogFile.parseFileName(fileName);
			if ( pair == null ) {
				continue;
			}
			
			boolean isWritable = ( i == fileNames.size() -1  ? true : false);
			long firstIndex = pair.first;
			long lastIndex = pair.second;
			int fileSize = 1024 * 1024 * 10;
			
			System.out.println("#WAL, fileName=" + fileName + ", firstIndex=" + firstIndex 
					+ ", lastIndex=" + lastIndex + ", isWritable=" + isWritable + ", snapshotIndex=" + snapshotIndex);
			//
			// 加载 logFile
			AbstractLogFile logFile = AbstractLogFile.create(walDir, firstIndex, lastIndex, fileSize, isWritable);
			AbstractLogFile.LogMetadata dataSet = logFile.openAtIndex( snapshotIndex );
			
			//
			start = dataSet.getStart();
			//
			if ( dataSet.getEntries() != null && !dataSet.getEntries().isEmpty() )
				entries.addAll( dataSet.getEntries() );
			//
			state = dataSet.getHs();
			
			if ( entries.size() > 3 )
				break;

		}
		
		if ( start != null ) 
			System.out.println(  ProtobufUtils.protoToJson(start) );
		
		if ( state != null )
			System.out.println(  ProtobufUtils.protoToJson(state) );
		
		if ( entries != null )
			for(Entry e: entries) {
				System.out.println(  ProtobufUtils.protoToJson(e) );
			}
		
	}

}
