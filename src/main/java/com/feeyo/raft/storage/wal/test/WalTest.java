package com.feeyo.raft.storage.wal.test;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.storage.wal.AbstractLogFile;
import com.feeyo.raft.storage.wal.proto.Walpb;
import com.feeyo.raft.util.Util;

public class WalTest {
	
	//
	public static void main(String[] args) throws Throwable {
		
		String walDir = "/Users/zhuam/Downloads/xx3";
		
		long snapshotIndex = -1;
		
		Walpb.Snapshot start = null;
		List<Entry> entries = new ArrayList<Entry>();
		Raftpb.HardState state = null;
		List<String> fileNames = Util.getSortedFileNamesInDirectory(walDir);
		for (int i = 0; i < fileNames.size(); i++) {
			String fileName = fileNames.get(i);
			long[] firstLastIndex = AbstractLogFile.parseFileName(fileName);
			if ( firstLastIndex == null ) {
				continue;
			}
			//
			boolean isWritable = ( i == fileNames.size() -1  ? true : false);
			long firstIndex = firstLastIndex[0];
			long lastIndex = firstLastIndex[1];
			int fileSize = 1024 * 1024 * 10;
			
			System.out.println("#WAL, fileName=" + fileName + ", firstIndex=" + firstIndex 
					+ ", lastIndex=" + lastIndex + ", isWritable=" + isWritable + ", snapshotIndex=" + snapshotIndex);
			//
			// 加载 logFile
			AbstractLogFile logFile = AbstractLogFile.create(walDir, firstIndex, lastIndex, fileSize, isWritable);
			AbstractLogFile.LogMetadata logMetadata = logFile.openAtIndex( snapshotIndex );
			start = logMetadata.getStart();
			if ( logMetadata.getEntries() != null && !logMetadata.getEntries().isEmpty() )
				entries.addAll( logMetadata.getEntries() );
			//
			state = logMetadata.getHs();
			//
			if (entries.size() > 3)
				break;

		}
		
		if (start != null)
			System.out.println(ProtobufUtils.protoToJson(start));

		if (state != null)
			System.out.println(ProtobufUtils.protoToJson(state));

		if (entries != null)
			for (Entry e : entries) {
				System.out.println(ProtobufUtils.protoToJson(e));
			}
		
	}

}
