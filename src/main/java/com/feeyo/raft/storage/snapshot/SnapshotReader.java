package com.feeyo.raft.storage.snapshot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.util.CrcUtil;
import com.feeyo.raft.util.IOUtil;

import static com.feeyo.raft.storage.snapshot.AbstractSnapshotter.FILE_COMPLETED;;

//
public class SnapshotReader implements AbstractSnapshotter.Readable {
	
	private static Logger LOGGER = LoggerFactory.getLogger( SnapshotReader.class );

	//
	private File file;
	private RandomAccessFile raf;

	private boolean isItCompleted = false;
	
	//
	public SnapshotReader(AbstractSnapshotter snapshotter, boolean isItRemote, long index, long term) {
		//
		String snapDir = isItRemote ? snapshotter.getRemoteSnapDir() : snapshotter.getLocalSnapDir();
		String fileName = snapshotter.toFileName(index, term);
		try {
			file = new File(snapDir, fileName);
			this.raf = new RandomAccessFile(file , "rw");
		} catch (FileNotFoundException e) {
			LOGGER.error("snapshot {} is not found", fileName);
		}
	}
	
	@Override
	public File getFile() {
		return file;
	}

	@Override
	public Snapshot next() throws RaftException {
		//
		Snapshot snapshot = readSnapshotInFile();
		if (snapshot == null)
			throw new RaftException(Errors.ErrEmptySnapshot);
		//
		return snapshot;
	}
	

	private Snapshot readSnapshotInFile() {
		//
		try {
	    	long crc32 = raf.readLong();			// crc32
	        int len = raf.readInt();				// dataLength
	        //
	        byte[] data = new byte[len];
	        int reads = raf.read(data);
	        if (reads != len) 
	            return null;
	        //
	        if (crc32 != CrcUtil.crc32(data)) 
	            return null;
	        //
	        Snapshot snap = Snapshot.parseFrom(data);
	        return snap;
	        
    	 } catch (Exception e) {
    		 LOGGER.warn("read snapshot err:", e);
    		 
    	 }
		 return null;
	}
	

	@Override
	public boolean isValid() {
		try {
			if (raf == null)
				return false;
			//
			if (!isItCompleted) {
				raf.seek(0);
				//
				// read head
				if (raf.readInt() == FILE_COMPLETED ) { 
					this.isItCompleted = true;
					//
					raf.readInt(); // skip isItRemote
					raf.readLong(); // skip lastSeqNo
				} else {
					return false;
				}
			}
			//
			if (this.raf.length() > this.raf.getFilePointer())
				return true;

		} catch (IOException e) { /* ignored */ }
		//
		return false;
	}
	
	//
	@Override
	public void close() throws IOException {
		IOUtil.closeQuietly( raf );
	}
}