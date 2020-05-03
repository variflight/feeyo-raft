package com.feeyo.raft.storage.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.storage.snapshot.AbstractSnapshotter;
import com.feeyo.raft.util.CrcUtil;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.Util;

import static com.feeyo.raft.storage.snapshot.AbstractSnapshotter.FILE_COMPLETED;
import static com.feeyo.raft.storage.snapshot.AbstractSnapshotter.FILE_NOT_COMPLETE;
import static com.feeyo.raft.storage.snapshot.AbstractSnapshotter.REMOTE_FLAG;
import static com.feeyo.raft.storage.snapshot.AbstractSnapshotter.LOCAL_FLAG;

//
public class SnapshotWriter implements AbstractSnapshotter.Writeable {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( SnapshotWriter.class );

	private AbstractSnapshotter snapshotter;
	protected String snapDir;
	private boolean isItRemote;
	//
	protected boolean isPrint = true;

	public SnapshotWriter(AbstractSnapshotter snapshotter,  boolean isItRemote) {
		this.snapshotter = snapshotter;
		this.isItRemote = isItRemote;
		this.snapDir = isItRemote ? snapshotter.getRemoteSnapDir() 	//
								: snapshotter.getLocalSnapDir();	//
	}

	//
	public void setPrint(boolean isPrint) {
		this.isPrint = isPrint;
	}

    protected String getSnapshotName(String fileName) {
        StringBuilder sb = new StringBuilder();
        sb.append(isItRemote ? "remote" : "local").append(" snapshot:").append(fileName);
        return sb.toString();
    }

	//
	// 保存快照
	@Override
	public void write(Snapshot snapshot) throws RaftException {
		//
		if ( Util.isEmptySnapshot(snapshot) )
			return;
		//
		long snapshotIndex = snapshot.getMetadata().getIndex();
		long snapshotTerm = snapshot.getMetadata().getTerm();
		long seqNo = snapshot.getChunk().getSeqNo();
		boolean last = snapshot.getChunk().getLast();
		//
		// local snapshot file 需要先存入 .temp ，完整后改名至 .snap
		String fileName = isItRemote ? snapshotter.toFileName(snapshotIndex, snapshotTerm)  //
								   : snapshotter.toTempFileName(snapshotIndex, snapshotTerm); //
		//
		if ( seqNo == 1 ) {
			//
			// delete old temp snapshot
			final File tempFile = new File(snapDir, fileName);
			if ( tempFile.exists() ) {
				try {
					FileUtils.forceDelete( tempFile );
				} catch (IOException e) {
					LOGGER.error("Fail to delete temp snapshot path {}", fileName);
				}
			}
		}

		String snapshotName = getSnapshotName(fileName);
		// the progress of the save
		if ( isPrint ) {
			//
			if (last) {
				LOGGER.info("{} {} is completed", snapshotName, seqNo);
			} else {
				if (seqNo == 1) {
					LOGGER.info("{} {} is started", snapshotName, seqNo);
				} else if (seqNo != 0 && seqNo % 100 == 0) {
					LOGGER.info("{} {}", snapshotName, seqNo);
				}
			}
		}

		// save file
		RandomAccessFile raf = null;
		File file = null;
		try {
			//
			boolean isWriteHead = false;
			file = new File(snapDir, fileName);
			if( !file.exists() ) {
				 isWriteHead = true;
				 file.createNewFile();
			}

			// write snap & head
			raf = new RandomAccessFile(file, "rw");
			if ( isWriteHead ) {
				raf.writeInt(FILE_NOT_COMPLETE);                            // 文件是否完整，0不完整，1完整
				raf.writeInt( isItRemote ? REMOTE_FLAG : LOCAL_FLAG ); 		// 是否远程快照
				raf.writeLong(0L);							// Snapshot lastSeqNo
			}
			raf.seek( raf.length() );						// 不能覆盖之前的内容
			//
			byte[] snapshotData = snapshot.toByteArray();	// write snapshot data
			raf.writeLong( CrcUtil.crc32(snapshotData) );
			raf.writeInt(snapshotData.length);
			raf.write(snapshotData);
            //
            // 如果是最后一块，设置完整标志
            if ( last ) {
        		raf.seek(0);
				raf.writeInt(FILE_COMPLETED);       // 设置文件完整
        		raf.writeInt( isItRemote ? REMOTE_FLAG : LOCAL_FLAG ); 		// 是否远程快照
        		raf.writeLong( seqNo );						// last seqNo
            }
		} catch (IOException e) {
			throw new RaftException("save snap err", e);

		} finally {
			IOUtil.closeQuietly( raf );
			//
			if ( last ) {
				//
				//  Rename local temporary snapshot
				if ( !isItRemote  ) {
					try {
						String destFileName = snapshotter.toFileName(snapshotIndex, snapshotTerm);
						FileUtils.moveFile(file, new File(snapDir, destFileName));
						LOGGER.info("rename, {} to {}", fileName, destFileName );
					} catch (IOException e) {
						LOGGER.error("rename err, {} {}", fileName, ExceptionUtils.getStackTrace(e) );
					}
				}
				//
				IOUtil.closeQuietly(this);
			}
		}
	}

	@Override
	public void close() throws IOException {
		//
	}
}