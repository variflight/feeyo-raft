package com.feeyo.raft.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.proto.Raftpb.MessageType;
import com.feeyo.raft.proto.Raftpb.Snapshot;


public class Util {
	
	// PayloadSize is the size of the payload of this Entry. Notably, it does not
	// depend on its Index or Term.
	public static int payloadSize(Entry e) {
		if ( e != null && e.getData() != null )
			return e.getData().size();
		return 0;
	}


	public static List<String> getFileNamesInDirectory(String dirPath) {
		return getFileNamesInDirectory(dirPath, (FilenameFilter)null);
	}

	public static List<String> getFileNamesInDirectory(String dirPath, FilenameFilter filter) {
		//
		File dir = new File(dirPath);
		if (!dir.exists() || !dir.isDirectory()) 
			return Collections.emptyList();
		//
		List<String> fileNames = new ArrayList<>();
		File[] files = dir.listFiles(filter);
		for (File file : files) {
			if (file.isFile()) {
				fileNames.add(file.getName());
			}
		}
		return fileNames;
	}

	public static List<String> getSortedFileNamesInDirectory(String dirPath, FilenameFilter filter) {
		//
		List<String> fileNames = getFileNamesInDirectory(dirPath, filter);
		if (fileNames.size() < 2)
			return fileNames;
		//
		Collections.sort(fileNames);
		return fileNames;
	}
	
	public static List<String> getSortedFileNamesInDirectory(String dirPath) {
		return getSortedFileNamesInDirectory(dirPath, null);
	}
	
	//
	public static void waitFor(long mills) {
		try {
			Thread.sleep(mills);
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	public static long min(long a, long b ) {
		if ( a > b ) 
			return b;
		return a;
	}
	
	public static long max(long a, long b ) {
		if ( a > b ) 
			return a;
		return b;
	}
	
	// MustSync returns true if the hard state and count of Raft entries indicate
	// that a synchronous write to persistent storage is required.
	public static boolean isMustSync(HardState st, HardState prevst, int entsnum) {
		//
		// Persistent state on all servers:
		// (Updated on stable storage before responding to RPCs)
		// currentTerm
		// votedFor
		// log entries[]
		return entsnum != 0 || ( st != null && prevst != null && 
				(st.getVote() != prevst.getVote() || 
				 st.getTerm() != prevst.getTerm() ||
				 st.getCommit() != prevst.getCommit() || 
				 st.getApplied() != prevst.getApplied() ) );
	}
	
	public static boolean isHardStateEqual(HardState st, HardState prevst) {
		//
		if ( prevst == null )
			return false;
		//
		return  st.getTerm() == prevst.getTerm() && 
				st.getVote() == prevst.getVote() && 
				st.getCommit() == prevst.getCommit() &&
			    st.getApplied() == prevst.getApplied() ;
	}
	
	public static boolean isEmptyHardState(HardState hs) {
		if ( hs == null )
			return true;
		return hs.getCommit() == 0 && hs.getVote() == 0 && hs.getTerm() == 0 && hs.getApplied() == 0;
	}
	
	// isEmptySnap returns true if the given Snapshot is empty.
 	public static boolean isEmptySnapshot(Snapshot snapshot) {
 		if( snapshot == null )
 			return true;
 		return snapshot.getMetadata().getIndex() == 0;
 	}
 	
	//
	public static boolean isLocalMsg(MessageType t) {
		//
	    if (t == MessageType.MsgHup ||
	    		t == MessageType.MsgBeat ||
	    		t == MessageType.MsgUnreachable ||
	    		t == MessageType.MsgSnapStatus ||
	    		t == MessageType.MsgCheckQuorum ) {
	    	return true;
	    }
	    return false;
	}
	
	public static boolean isResponseMsg(MessageType t) {
		//
	    if (t == MessageType.MsgAppendResponse ||
	    		t == MessageType.MsgRequestVoteResponse ||
	    		t == MessageType.MsgHeartbeatResponse ||
	    		//t == MessageType.MsgUnreachable ||
	    		t == MessageType.MsgRequestPreVoteResponse ) {
	    	return true;
	    }
	    return false;
	}
	
	
	// voteResponseType maps vote and prevote message types to their corresponding responses.
	public static MessageType voteRespMsgType(MessageType type) throws RaftException {
		switch(type) {
		case MsgRequestVote:
			return MessageType.MsgRequestVoteResponse;
		case MsgRequestPreVote:
			return MessageType.MsgRequestPreVoteResponse;
		default:
			throw new Errors.RaftException("not a vote message: " + type);
		}
	}
	
	// --------------------------------- toStr --------------------------------------------
	// 
	public static String toStr(Entry ent) {
		if ( ent == null ) {
			return "empty";
		} else  {
			StringBuffer entStr = new StringBuffer();
			entStr.append("term:").append( ent.getTerm() );
			entStr.append(",index:").append( ent.getIndex() );
			return entStr.toString();
		}
	}
	
	public static String toStr(List<Entry> ents) {
		if ( ents == null || ents.isEmpty() ) {
			return "empty";
		} else  {
			StringBuffer entStr = new StringBuffer();
			entStr.append("[");
			entStr.append("first(term:").append( ents.get(0).getTerm() );
			entStr.append(",index:").append( ents.get(0).getIndex() );
			entStr.append(")");
			
			entStr.append(" last(term:").append( ents.get( ents.size() - 1 ).getTerm() );
			entStr.append(",index:").append( ents.get( ents.size() - 1 ).getIndex() );
			entStr.append(")");
			
			entStr.append(" size:").append( ents.size() );
			entStr.append("]");
			return entStr.toString();
		}
	}
	
	public static String toStr(HardState hs) {
		return hs != null ? ProtobufUtils.protoToJson(hs) : "empty";
	}
	
	public static String toStr(ConfState cs) {
		return cs != null ? ProtobufUtils.protoToJson(cs) : "empty";
	}
	
	// -------------------------------------------------------------------------------------------- 
	
	public static <T> boolean isEmpty(List<T> list) {
		return list == null || list.isEmpty();
	}

	public static <T> boolean isNotEmpty(List<T> list) {
		return list != null && !list.isEmpty();
	}

	public static <T> int len(List<T> list) {
		return list == null ? 0 : list.size();
	}
	
//	public static <T> CompositeList<T> slice(CompositeList<T> raw, int lo, int hi) {
//		//
//		if (hi <= lo)
//			return null;
//		//
//		CompositeList<T> list = new CompositeList<T>();
//		for (int i = lo; i < hi; i++)
//			list.add(raw.get(i));
//		return list;
//	}

	public static <T> List<T> slice(List<T> raw, int lo, int hi) {
		//
		if (hi <= lo)
			return null;
		//
		List<T> list = new ArrayList<T>(hi - lo);
		for (int i = lo; i < hi; i++)
			list.add(raw.get(i));
		return list;
	}

	public static List<Entry> limitSize(List<Entry> ents, long maxSize) {
		//
		int l = len(ents);
		if ( l == 0 )
			return ents;
		//
		int size = ents.get(0).getSerializedSize();
		int limit;
		for (limit = 1; limit < len(ents); limit++) {
			size += ents.get( limit ).getSerializedSize();
			if (size > maxSize )
				break;
		}
		// 优化
		if ( limit == (l - 1) )
			return ents;
		//
		return slice(ents, 0, limit);
	}

	public static String getStackTrace() {
		StackTraceElement stack[] = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		for (int i = 0; i < stack.length; i++) {
			sb.append("      at ").append( stack[i].getClassName() ).append(".").append( stack[i].getMethodName());
			sb.append("(").append( stack[i].getFileName()).append(":").append( stack[i].getLineNumber() ).append(")");
			sb.append("\n");
		}
		return sb.toString();
	}

	public static long monotonicUs() {
		return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
	}
}