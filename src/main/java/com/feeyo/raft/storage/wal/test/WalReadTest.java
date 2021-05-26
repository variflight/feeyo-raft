package com.feeyo.raft.storage.wal.test;

import java.util.List;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.EntryType;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Newdbpb.ProposeCmd;
import com.feeyo.raft.storage.wal.Wal;
import com.feeyo.raft.util.Pair;

public class WalReadTest {
	
	public static void main(String[] args) throws Throwable {
		
		 ProtobufDecoder<ProposeCmd> protobufDecoder = 
				new ProtobufDecoder<>(ProposeCmd.getDefaultInstance(), true);
		
		String path = "/Users/zhuam/Downloads/xx3";
		System.out.println( path );
		
		boolean syncLog = true;
		
		Wal wal = new Wal(path, 1024 * 1024 * 10, syncLog);
		Pair<List<Entry>, HardState> pair = wal.readAll(0); //15473
		List<Entry> ents = pair.first;
		HardState lastHs = pair.second;
		
		
		if ( ents != null ) {
			
			for(Entry ent: ents) {
				long term = ent.getTerm();
				long idx = ent.getIndex();
				EntryType type = ent.getEntryType();
				
				if ( ent.getData() != null  ) {
					
					byte[] data = ent.getData().toByteArray();
					
					if ( type == EntryType.EntryNormal ) {

						try {
							//
							List<ProposeCmd> propseCmds = protobufDecoder.decode(data);
							if ( propseCmds == null ) {
								//System.out.println(idx + ", " + term + ", " + propseCmds.size() ); 
							//} else {
								System.out.println(term + ", " + idx + ", " + term + ", " + "codec nulllllll");
							} 
						} catch (IndexOutOfBoundsException e1) {
							e1.printStackTrace();
						}
						
					} else {
						
						System.out.println(term + ", " + idx  + ", " + type + ", " + data.length +  "， xx---- " + new String( data) ); 
					}
					
				} else {
					
					System.out.println(term + ", " + idx  + ", " + type  + ", " + term + ",  data null "  );
				}
			}
			
		}
		
		if ( lastHs != null )
			System.out.println("commit=" + lastHs.getCommit());
		
		wal.stop();
		
	}

}
