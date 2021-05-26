package com.feeyo.raft.storage.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.CrcUtil;
import com.feeyo.raft.util.IOUtil;

public class DefaultSnapshotter extends AbstractSnapshotter {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( DefaultSnapshotter.class );
	
	//
	private static final AtomicBoolean isCleaning = new AtomicBoolean(false);
	protected static List<String> snapshotDirs = new CopyOnWriteArrayList<>();

	//
	public DefaultSnapshotter(String snapDir) {
		super( snapDir );
		snapshotDirs.add( localSnapDir );
	}
	
	//
	// 删除过期的历史快照文件
	@Override
	public void gc() {
		//
		if ( !isCleaning.compareAndSet(false, true) )
			return;
		//
		try {
			// 升序排列
			for(String snapDir: snapshotDirs) {
				List<String> fileNames = getFileNamesOfDir( snapDir );
				if ( fileNames.size() < 2 )
					continue;
				//
				for(int i = 0; i < fileNames.size() - 1; i++ ) {
					String fileName = fileNames.get(i);
					FileUtils.forceDelete( new File( snapDir, fileName) );
				}
			}
			
		} catch(Throwable e) {
			if ( Errors.ErrNoSnapshot.equalsIgnoreCase( e.getMessage() ) ) 
				return;
			//
			LOGGER.warn("delete expired snap err:", e);
		} finally {
			isCleaning.set(false);
		}
	}


	@Override
	public SnapshotMetadata getMetadata() {
		//
		// Last snapshot file
		String fileName = null;
		try {
			// 升序排列
			List<String> fileNames = getFileNamesOfDir( localSnapDir );
			fileName = fileNames.get( fileNames.size() - 1 );
		} catch (RaftException e1) {
			if (e1.getMessage().equals(Errors.ErrNoSnapshot))
				return null;
		}
		
		
		//
		SnapshotMetadata metadata = null;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(new File(localSnapDir, fileName), "r");
			raf.seek(0);
			//
			if (raf.readInt() == FILE_COMPLETED) {						//
				raf.readInt();								// skip isRemoteSnapshot
				long lastSeqNo = raf.readLong();			// skip lastSeqNo
				for (int i = 1; i < lastSeqNo; i++) {		// skip
					raf.readLong();							// skip crc32
					int dataLength = raf.readInt();			// skip length & data
					raf.skipBytes(dataLength);
				}
				//
				long crc32 = raf.readLong();
				int dataLen = raf.readInt();
				//
				byte[] data = new byte[ dataLen ];
				int reads = raf.read(data);
				if (reads != dataLen) 
					return null;
				//
				if ( crc32 != CrcUtil.crc32(data)) 
					return null;
				//
				Snapshot lastSnapshot = Snapshot.parseFrom(data);
				if (lastSnapshot != null) 
					metadata = lastSnapshot.getMetadata();
			}

		} catch (Exception e) {
			LOGGER.error("get meta err, fileName={}, ex={}", fileName, ExceptionUtils.getStackTrace(e));
		} finally {
			IOUtil.closeQuietly( raf );
		}
		//
		return metadata;
	}

	@Override
	public AbstractSnapshotter.Writeable create(boolean isItRemote) {
		return new SnapshotWriter(this, isItRemote);
	}

	//
	@Override
	public AbstractSnapshotter.Readable open(boolean isItRemote, long index, long term) {
		return new SnapshotReader(this, isItRemote, index, term);
	}

	//
	@Override
	public void moveFile(AbstractSnapshotter.Readable readable) {
		//
		File srcFile = readable.getFile();
		try {
			FileUtils.moveFile(srcFile,  new File(localSnapDir, srcFile.getName()));
		} catch (IOException e) {
			LOGGER.error("Fail to move file {}", srcFile.getName());
		}
	}
}