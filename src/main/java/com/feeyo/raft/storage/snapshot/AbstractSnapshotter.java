package com.feeyo.raft.storage.snapshot;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.Util;

/**
 * Used for reading and writing snapshots
 * 
 * @author zhuam
 *
 */
public abstract class AbstractSnapshotter {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractSnapshotter.class );
	
	//
	public static final int FILE_NOT_COMPLETE = 0;
	public static final int FILE_COMPLETED = 1;
	//
	public static final int LOCAL_FLAG = 0;
	public static final int REMOTE_FLAG = 1;
	
	//
	// temporary
	protected static final String TEMPORARY_SUFFIX = ".temp";
	protected static final String SNAPSHOT_SUFFIX = ".snap";
	
	
	protected String localSnapDir;
	protected String remoteSnapDir;
	
	//
	public AbstractSnapshotter(String snapDir) {
		//
		try {
			this.localSnapDir =  snapDir + File.separator + "local_snap" ;
			FileUtils.forceMkdir( new File( localSnapDir ) );
		} catch (IOException e) {
			 LOGGER.error("Fail to create directory {}", localSnapDir);
		}

		//
		try {
			this.remoteSnapDir = snapDir + File.separator + "remote_snap" ;
			FileUtils.forceMkdir( new File( remoteSnapDir ) );
		} catch (IOException e) {
			 LOGGER.error("Fail to create directory {}", remoteSnapDir);
		}
	}
	
	public String getLocalSnapDir() {
		return localSnapDir;
	}
	
	public String getRemoteSnapDir() {
		return remoteSnapDir;
	}
	
	//
	public String toTempFileName(long index, long term) {
		return String.format("%016d-%016d%s", index, term, TEMPORARY_SUFFIX);
	}
	
	public String toFileName(long index, long term) {
		return String.format("%016d-%016d%s", index, term, SNAPSHOT_SUFFIX);
	}
	
	//
	protected List<String> getFileNamesOfDir(String dir) throws RaftException {
		//
		List<String> fileNames = Util.getFileNamesInDirectory( dir );
		Iterator<String> iter = fileNames.iterator();
		while( iter.hasNext() ) {
			// 后缀的合法性检查
			String name = iter.next();
			if( !name.endsWith(SNAPSHOT_SUFFIX) ) 
				iter.remove();
		}
		
		//  If there is no available snapshots, an ErrNoSnapshot will be returned
		if ( fileNames.isEmpty() )
			throw new Errors.RaftException( Errors.ErrNoSnapshot );
		
		// the filename of the snapshots in logical time order (from newest to oldest)
		Collections.sort( fileNames );
		return fileNames;
	}
	
	//
	// --------------------------- abstract method -----------------------------------
	//
	
	// Snapshot GC for destroy expired garbage files
	public abstract void gc();
	
	//
	// Meta of the latest snapshot
	public abstract SnapshotMetadata getMetadata();
	
	//
	// Create a snapshot writer
	public abstract Writeable create(boolean isItRemote);

	//
	// Open a snapshot reader.
	public abstract Readable open(boolean isItRemote, long index, long term);
	
	//
	// Move remote to local
	public abstract void moveFile(Readable readable);
	
	//
	// ----------------------------- reader & writer interface ---------------------------------
	//
	public interface Writeable extends Closeable  {
		//
		public void write(Snapshot snapshot) throws RaftException;
	}
	
	//
	public interface Readable extends Closeable {
		//
		public File getFile();
		//
		public Snapshot next() throws RaftException;
		public boolean isValid();
	}

}