package com.feeyo.raft.storage.wal.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.storage.wal.Wal;
import com.feeyo.raft.util.Util;

public class WalFlushbackTest {
	
	public static void main(String[] args) throws Throwable {
		
		boolean syncLog = false;
		
		
		String path = System.getProperty("user.dir") + File.separator + "wal_test";
		System.out.println( path );
		
		//
		Wal wal = new Wal(path, 1024 * 20,  syncLog);
		Wal.ReadAllLogs walLogs = wal.readAll( -1 );
		
		long lastIndex = Util.isEmpty(walLogs.getEntries()) ? 1 : walLogs.getHs().getCommit();
		lastIndex++;
		
		System.out.println("xxxx" + lastIndex);
		
		for(int i = 466; i<=467; i++) {
			Entry entry = Entry.newBuilder()
					.setTerm( 12 )
					.setIndex( i )
					.build();
	
			List<Entry> entries = new ArrayList<Entry>();
			entries.add(entry) ;
			
			HardState hs = HardState.newBuilder()
					.setTerm(12)
					.setVote(88)
					.setCommit(i)
					.build();
	
			wal.save(entries, hs);
		}
		
		//
		wal.stop();
	}

}
