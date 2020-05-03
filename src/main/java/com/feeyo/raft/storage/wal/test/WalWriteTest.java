package com.feeyo.raft.storage.wal.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.storage.wal.Wal;
import com.feeyo.raft.util.Util;

public class WalWriteTest {
	
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
		
		for(long i = lastIndex ; i < lastIndex + 1300; i++) {
			
			Entry entry = Entry.newBuilder()
							.setTerm( i )
							.setIndex(i)
							.build();
			
			List<Entry> entries = new ArrayList<Entry>();
			entries.add(entry) ;
			
			HardState hs = HardState.newBuilder()
					.setTerm(11)
					.setVote(88)
					.setCommit(i)
					.build();
			
			System.out.println(i);
			
			wal.save(entries, hs);
			
		}
		
		wal.stop();
		
		
	}

}
