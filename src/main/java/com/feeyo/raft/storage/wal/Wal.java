package com.feeyo.raft.storage.wal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.storage.wal.proto.Walpb;
import com.feeyo.raft.util.Util;
import com.feeyo.raft.util.Pair;
import com.feeyo.raft.util.ProtobufCodedOutputUtil;

// Write Ahead Log
//	
// @see https://github.com/etcd-io/etcd/blob/master/wal/wal.go
// 
// E.g: 
//      0000000000000001-0000000000001000: closed
//		0000000000001001-0000000000002000: closed
//      0000000000002001-0000000000000000: open
//
// @author zhuam
//
public class Wal {
	
	private static Logger LOGGER = LoggerFactory.getLogger(Wal.class);
	//
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock writeLock = this.readWriteLock.writeLock();
	private final Lock readLock = this.readWriteLock.readLock();
	//
	private int maxFileSize;
	private String walDir;
	//
	private CopyOnWriteArrayList<AbstractLogFile> logFiles = new CopyOnWriteArrayList<AbstractLogFile>(); // 所有日志文件
	private Walpb.Snapshot start; // 每次读取WAL文件时，并不会从头开始读取。而是根据这里的快照数据定位到位置。 Snapshot.Index记录了对应快照数据的最后一条entry记录的索引，而Snapshot.Term记录的是对应记录的任期号。
	private HardState state = null; // 每次写入entryType类型的记录之后，都需要追加一条stateType类型的数据，state成员就用于存储当前状态
	private Entry lastWroteEntry = null; // 用于连续性校验

	//
	public Wal(String path, int maxFileSize, boolean syncLog) {	
		this.maxFileSize = maxFileSize;
		this.walDir = path + File.separator + "wal";
		File f = new File(walDir);
		if (!f.exists()) 
			f.mkdirs();
		// default walSnap 
		start = Walpb.Snapshot.newBuilder() //
				.setIndex(0) //
				.setTerm(0) //
				.build(); //
	}
	//
	public void stop() {
		// force flush disk
		AbstractLogFile logFile = getLastLogFile();
    	if ( logFile != null ) {
    		logFile.flush();
    		LOGGER.warn("flush disk, name={}", logFile.toString());
    	}
	}
	//
	public Walpb.Snapshot getStart() {
		return start;
	}
	//
	// readAll负责从当前WAL文件目录读取所有的记录
	public Pair<List<Entry>, HardState> readAll(long snapshotIndex) throws Throwable {
		readLock.lock();
		try {
			List<Entry> entries = new ArrayList<Entry>();
			HardState lastHs = null;
			List<String> fileNames = Util.getSortedFileNamesInDirectory(walDir);	// 返回目录中所有WAL文件名
			for (int i = 0; i < fileNames.size(); i++) {
				String fileName = fileNames.get(i);
				long[] firstLastIndex = AbstractLogFile.parseFileName(fileName);
				if (firstLastIndex == null) {
					LOGGER.warn("readAll file [ {} ] is not valid", fileName);
					continue;
				}
				//
				boolean isWritable = i == fileNames.size() - 1;
				long firstIndex = firstLastIndex[0];
				long lastIndex = firstLastIndex[1];
				//
				// Open file
				AbstractLogFile logFile = AbstractLogFile.create(walDir, firstIndex, lastIndex, maxFileSize, isWritable);
				AbstractLogFile.LogMetadata logMetadata = logFile.openAtIndex(snapshotIndex);
				if (logMetadata.getStart() != null) // WalSnap
					start = logMetadata.getStart();
				//
				if (Util.isNotEmpty(logMetadata.getEntries())) // Entries
					entries.addAll(logMetadata.getEntries());
				//
				if (logMetadata.getHs() != null) // HardState
					lastHs = logMetadata.getHs();
				//
				if (!isWritable)
					logFile.close();
				//
				logFiles.add( logFile );
			}
			//
			if (Util.isNotEmpty(entries))
				lastWroteEntry = entries.get(entries.size() - 1);
			//
			return Pair.create(entries, lastHs);
			
		} finally {
			readLock.unlock();
		}
	}
	
	//
	public void save(List<Entry> entries, HardState hs) throws RaftException {
		writeLock.lock();
		try {
			long t1 = TimeUtil.currentTimeMillis();
			boolean mustSync = Util.isMustSync(hs, this.state, Util.isNotEmpty(entries) ? entries.size() : 0); 	// 这些数据是否需要落盘
			saveEntry(entries); // 保存 entries
			saveState(hs); // 保存 committed 状态信息
			//
			long t2 = TimeUtil.currentTimeMillis();
			if (mustSync) {
				// 同步刷盘
				AbstractLogFile logFile = getLastLogFile();
				if (logFile != null)
					logFile.flush();
			}
			//
			long t3 = TimeUtil.currentTimeMillis();
			long elapsed =  t3 - t1;
			if (elapsed > 250) {
				LOGGER.info("wal save slow, t3-t1={}, t3-t2={}, t2-t1={}", (t3 - t1), (t3 - t2), (t2 - t1));
			}

		} catch (Exception e) {
			throw new Errors.RaftException(e);
		} finally {
			writeLock.unlock();
		}
	}
	
	//
	// ------------------------------------------------ save ------------------------------------------------
	//
	private void saveEntry(List<Entry> entries) throws RaftException {
		if ( Util.isEmpty( entries ))
			return;
		//
		int nextWrittenIdx = 0;
		while (nextWrittenIdx < entries.size()) {
			//
			// 连续性检测,解决冲突
			Entry entry = entries.get(nextWrittenIdx);
			if (lastWroteEntry != null && entry.getIndex() - lastWroteEntry.getIndex() != 1) {
				//
				LOGGER.warn("discontinuity err: lastEntry=[term:{}, index:{}], newEntry=[term:{}, index:{}]", 
						lastWroteEntry != null ? lastWroteEntry.getTerm() : null,  
						lastWroteEntry != null ? lastWroteEntry.getIndex() : null,
						entry != null ? entry.getTerm() : null,
						entry != null ? entry.getIndex() : null);
				//
				// 回退至 To be append entry 的前一个
				long lastIndexKept = entry.getIndex() - 1;
				truncateSuffix(lastIndexKept);
			}
			//
			ByteBuffer buffer = null;
			try {
				buffer = ProtobufCodedOutputUtil.msgToBuffer(entry);
				buffer.flip();
				//
				AbstractLogFile logFile = getOrCreateLogFile(buffer.limit());
				logFile.append(AbstractLogFile.DataType.ENTRY, buffer);
				logFile.setLastLogIndex(entry.getIndex());
				lastWroteEntry = entry;
				nextWrittenIdx++;
			} finally {
				if (buffer != null)
					NetSystem.getInstance().getBufferPool().recycle(buffer);
			}
			//
			if (LOGGER.isDebugEnabled() )
				LOGGER.debug("save entry, ent index={}", entry.getIndex());
		}
	}
	
	private void saveState(HardState hs) throws RaftException {
		if (!Util.isEmptyHardState(hs) && !Util.isHardStateEqual(hs, this.state)) {
			ByteBuffer buffer = null;
			try {
				buffer = ProtobufCodedOutputUtil.msgToBuffer(hs);
				buffer.flip();
				//
				AbstractLogFile logFile = getOrCreateLogFile(buffer.limit());
				logFile.append(AbstractLogFile.DataType.STATE, buffer);

				if (LOGGER.isDebugEnabled())
					LOGGER.debug("save hs, hs={}", ProtobufUtils.protoToJson(hs));
				//
				this.state = hs;
			} finally {
				if (buffer != null)
					NetSystem.getInstance().getBufferPool().recycle(buffer);
			}
		}
	}
	
	//
	private void saveSnapshot(Walpb.Snapshot snapshot) throws RaftException {
		byte[] data = snapshot.toByteArray();
		AbstractLogFile logFile = getOrCreateLogFile( data.length );
		logFile.append(AbstractLogFile.DataType.SNAPSHOT, snapshot.toByteArray());
		logFile.flush();
		//
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("save snapshot, snap={}", ProtobufUtils.protoToJson(snapshot));
	}
	
	//
	// --------------------------------------------- Truncate ---------------------------------------------
	//
	// 将 firstIndexKept 之前的 logFile 全部删除
	private void truncatePrefix(long firstIndexKept) {
		//
		LOGGER.info("truncatePrefix, firstIndexKept={}", firstIndexKept);
		//
		while (!logFiles.isEmpty()) {
			AbstractLogFile logFile = getFirstLogFile();
			if (logFile != null && !logFile.isWritable() && logFile.getLastLogIndex() < firstIndexKept) {
				logFile.delete();
				logFiles.remove( logFile );
			} else {
				break;
			}
		}
	}
	
	// 用于删除未达成一致的 Entry, 根据 lastIndexKept 找到对应的文件偏移，然后截断文件, 注：需要考虑跨多个文件
	private void truncateSuffix(long lastIndexKept) {
		// About logFile history
		// Eg:
		// 001-100
		// 101-200
		// 201-000 ( last=208 )
		//
		// 如下代码针对 lastIndexKept=(198, 200, 201, 205) 这几种情况的处理
		//
		int lastfIdx = logFiles.size() - 1;
		for(int i = lastfIdx; i >= 0; i--) {
			AbstractLogFile logFile = logFiles.get( i );
			if (lastIndexKept == logFile.getLastLogIndex()) {
				break;
			} else if (lastIndexKept < logFile.getFirstLogIndex()) {
				// delete file
				logFile.delete();
				logFiles.remove(logFile);
				//
			} else if (lastIndexKept < logFile.getLastLogIndex()) {
				// truncate
				logFile.truncate(lastIndexKept);
			}
		}
	}
	
	//
	public void saveSnapMeta(Raftpb.SnapshotMetadata snapMeta) throws RaftException {
		saveSnapMeta(snapMeta.getIndex(), snapMeta.getTerm());
	}

	public void saveSnapMeta(long index, long term) throws RaftException {
		writeLock.lock();
		try {
			//
			// must save the snapshot index to the WAL before saving the
			// snapshot to maintain the invariant that we only Open the
			// wal at previously-saved snapshot indexes.
			//
			long firstIndex = this.start.getIndex();
			if (index <= firstIndex) {
				throw new Errors.RaftException(Errors.ErrCompacted);

			} else {
				// WAL snapshot
				Walpb.Snapshot walSnap = Walpb.Snapshot.newBuilder() //
						.setIndex( index ) //
						.setTerm( term ) //
						.build(); //
				//
				saveSnapshot(walSnap);
				this.start = walSnap;
				truncatePrefix(index);
			}
		} finally {
			writeLock.unlock();
		}
	}
	
	private AbstractLogFile getFirstLogFile() {
		if (this.logFiles.isEmpty())
			return null;
		return this.logFiles.get(0);
	}

	private AbstractLogFile getLastLogFile() {
		if (this.logFiles.isEmpty())
			return null;
		return this.logFiles.get(this.logFiles.size() - 1);
	}

	private AbstractLogFile getOrCreateLogFile(int waitToWroteSize) throws RaftException {
		AbstractLogFile logFile = null;
		try {
			logFile = getLastLogFile();
			if (logFile != null && logFile.isWritable())
				if (logFile.remainingBytes(waitToWroteSize) > 0)
					return logFile;
				else 
					logFile.cut();
			//
			// 创建下一个文件
			long newFirstLogIndex = logFile == null ? 0 : logFile.getLastLogIndex() + 1;
			long newLastLogIndex = 0;
			boolean isWriteable = true;
			logFile = AbstractLogFile.create(walDir, newFirstLogIndex, newLastLogIndex, maxFileSize, isWriteable);
			logFiles.add(logFile);
			//
			// 新文件第一条数据需要含 snapshot 信息
			saveSnapshot(start);
			//
		} catch(Exception ex) {
			throw new RaftException( ex );
		}
		return logFile;
	}
}