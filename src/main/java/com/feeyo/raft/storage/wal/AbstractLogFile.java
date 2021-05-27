package com.feeyo.raft.storage.wal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.storage.wal.proto.Walpb;
import com.feeyo.raft.storage.wal.proto.Walpb.Snapshot;

public abstract class AbstractLogFile {
	protected String path; 						// 存储路径
	protected String fileName; 					// 文件名
	protected int fileSize; 					// 文件尺寸
	protected File file;						//
	protected volatile boolean isWritable; 		// 是否可写
	protected long firstLogIndex;				//
	protected long lastLogIndex;
	//
	public abstract LogMetadata openAtIndex(long index) throws RaftException;  // 寻找snapshot 以后的日志文件并打开
	public abstract boolean append(int dataType, byte[] data);	// 追加
	public abstract boolean append(int dataType, ByteBuffer dataBuffer);
	public abstract void truncate(long index);	// 从给定索引中删除文件中的数据
	public abstract void cut();	// 文件切换(TODO: 关闭当前file，重命名)
	//
	public abstract int remainingBytes(int size);
	public abstract int flush();
	public abstract void delete();
	public abstract void close();
	//
	public boolean isWritable() {
		return isWritable;
	}
	//
	public long getFirstLogIndex() {
		return firstLogIndex;
	}
	//
	public long getLastLogIndex() {
		return lastLogIndex;
	}
	//
	public void setLastLogIndex(long lastLogIndex) {
		this.lastLogIndex = lastLogIndex;
	}
	///
	public static AbstractLogFile create(String walDir, long firstLogIndex, long lastLogIndex, int fileSize, boolean isWritable)
			throws IOException {
		AbstractLogFile logFile = new LogFile(walDir, firstLogIndex, lastLogIndex, fileSize, isWritable);
		return logFile;
	}

	// 根据 firstIndex、lastIndex 生成文件名
	public static String generateFileName(long firstIndex, long lastIndex) {
		return String.format("%016d-%016d", firstIndex, lastIndex);
	}

	// 解析文件名
	public static long[] parseFileName(String fileName) {
		String[] nameArr = fileName.split("-");
		if (nameArr.length != 2)
			return null;
		try {
			long firstIndex = Long.parseLong(nameArr[0]);
			long lastIndex = Long.parseLong(nameArr[1]);
			return new long[] {firstIndex, lastIndex};
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	//
	// -------------------------Log Metadata-----------------------------
	public static class LogMetadata {
		private Walpb.Snapshot start;
		private List<Entry> entries;
		private HardState hs;

		public LogMetadata(Snapshot start, List<Entry> entries, HardState hs) {
			this.start = start;
			this.entries = entries;
			this.hs = hs;
		}

		public Walpb.Snapshot getStart() {
			return start;
		}

		public List<Entry> getEntries() {
			return entries;
		}

		public HardState getHs() {
			return hs;
		}
	}

	//
	// ---------------------- Data Type---------------------------------
	public static class DataType {
		public static final int ENTRY = 1;
		public static final int STATE = 2;
		public static final int SNAPSHOT = 3;
	}

}