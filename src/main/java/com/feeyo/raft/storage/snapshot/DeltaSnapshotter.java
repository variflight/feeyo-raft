package com.feeyo.raft.storage.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Newdbpb;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.ConfState;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.proto.Raftpb.SnapshotMetadata;
import com.feeyo.raft.util.CrcUtil;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.Util;

/**
 * 增量快照
 */
public class DeltaSnapshotter extends AbstractSnapshotter {

    private static Logger LOGGER = LoggerFactory.getLogger(DeltaSnapshotter.class);

    private static final String DELTA_SUFFIX = ".delta";
    //
    private String deltaDir;
    //
    // 限制快照写入吞吐为50MB/s
    private static final long throttleThroughputBytes = 50L * 1024 * 1024;
    private static final SnapshotThrottle snapshotThrottle = new ThroughputSnapshotThrottle(throttleThroughputBytes, 1);

    //
    private DefaultSnapshotter delegate;

    public DeltaSnapshotter(String snapDir) {
        super(snapDir);
        //
        try {
            this.deltaDir = snapDir + File.separator + "delta_snap";
            FileUtils.forceMkdir(new File(deltaDir));
        } catch (IOException e) {
            LOGGER.error("Fail to create directory {}", deltaDir);
        }
        //
        this.delegate = new DefaultSnapshotter(snapDir);
    }


    private List<Newdbpb.ColumnFamilyHandle> allCfhs;

    /*
     * 协商
     * @param allCfh
     * @return true 表示同意增量， false 要求全量
     */
    public boolean handleDeltaNegotiation(List<Newdbpb.ColumnFamilyHandle> allCfh) {
        //
        if (allCfhs == null) {
            this.allCfhs = allCfh;
            return false;
        }
        //
        // Check delta file
        for (Newdbpb.ColumnFamilyHandle cfh : allCfh) {
            File file = new File(deltaDir, cfh + DELTA_SUFFIX);
            if (!file.exists())
                return false;
        }

        return true;
    }

    /*
     * 处理增量数据
     */
    public void handleDelta(Newdbpb.ColumnFamilyHandle cfh, Snapshot snapshot) throws RaftException {
        //
        // save
        if (Util.isEmptySnapshot(snapshot))
            return;
        //
        long seqNo = snapshot.getChunk().getSeqNo();
        boolean last = snapshot.getChunk().getLast();
        //
        // cfh snapshot file 需要先存入 .temp ，完整后改名至 .delta
        String fileName = cfh + TEMPORARY_SUFFIX; //
        //
        if (seqNo == 1) {
            // delete old temp snapshot
            final File tempFile = new File(deltaDir, fileName);
            if (tempFile.exists()) {
                try {
                    FileUtils.forceDelete(tempFile);
                } catch (IOException e) {
                    LOGGER.error("Fail to delete temp snapshot path {}", fileName);
                }
            }
        }
        //
        // the progress of the save
        if (last) {
            LOGGER.info("saved delta {} {} is completed", fileName, seqNo);
        } else {
            if (seqNo == 1) {
                LOGGER.info("saved delta {} {} is started", fileName, seqNo);
            } else if (seqNo != 0 && seqNo % 100 == 0) {
                LOGGER.info("saved delta {} {}", fileName, seqNo);
            }
        }

        // save file
        RandomAccessFile raf = null;
        File tempFile = null;
        try {
            //
            boolean isWriteHead = false;
            tempFile = new File(deltaDir, fileName);
            if (!tempFile.exists()) {
                isWriteHead = true;
                tempFile.createNewFile();
            }

            //
            // Write snapshots with headers and data
            raf = new RandomAccessFile(tempFile, "rw");
            if (isWriteHead) {
            	// snapshotHead
                raf.writeInt(FILE_NOT_COMPLETE);     // 文件是否完整，0不完整，1完整
                raf.writeInt(LOCAL_FLAG);            // 是否远程快照
                raf.writeLong(0L);                   // Snapshot lastSeqNo
            }
            //
            // 不能覆盖之前的内容
            raf.seek(raf.length());
            //
            // snapshotData
            byte[] snapshotData = snapshot.toByteArray();
            raf.writeLong(CrcUtil.crc32(snapshotData));
            raf.writeInt(snapshotData.length);
            raf.write(snapshotData);
            //
            // 如果是最后一块，设置完整标志
            if (last) {
                raf.seek(0);
                raf.writeInt(FILE_COMPLETED);       // 设置文件完整
                raf.writeInt(LOCAL_FLAG);           // 是否远程快照
                raf.writeLong(seqNo);               // last seqNo
            }
            //
        } catch (IOException e) {
            throw new RaftException("save delta err", e);
        } finally {
        	//
			IOUtil.closeQuietly(raf);
			raf = null;
			//
			if (last) {
				String destFileName = cfh + DELTA_SUFFIX;
				try {
					File destFile = new File(deltaDir, destFileName);
					if (destFile.exists())
						FileUtils.forceDelete(destFile);
					//
					FileUtils.moveFile(tempFile, destFile);
					LOGGER.info("rename {} to {}", fileName, destFileName);
					//
				} catch (IOException e) {
					LOGGER.error("delta err {}, {}", fileName, destFileName, e);
					throw new RaftException("save delta err", e);
				}
			}
		}
    }

    /*
     * 增量数据处理完毕
     */
    public void handleDeltaEnd(long index, long term, ConfState confState) throws RaftException {
        //
        // Merge all column family handles
        //
        List<File> deltaFiles = new ArrayList<>( allCfhs.size() );
        for (Newdbpb.ColumnFamilyHandle cfh : allCfhs) {
            String fileName = cfh + DELTA_SUFFIX;
            File file = new File(deltaDir, fileName);
            if (!file.exists())
                throw new RaftException("delta error: cfh(" + cfh + ") is missing its snapshot");
            //
            deltaFiles.add(file);
        }

        //
        LOGGER.info("merge delta is started, index:{}, term:{}", index, term);

        //
        File tmpLocalSnapFile;
        RandomAccessFile tmpLocalSnapRaf = null;
        long globalSeqNo = 0L;
        //
        try {
            tmpLocalSnapFile = new File( delegate.getLocalSnapDir(), toTempFileName(index, term) );
            if (tmpLocalSnapFile.exists())
                tmpLocalSnapFile.delete();
            tmpLocalSnapFile.createNewFile();
            //
            // Write snapshots with headers and data
            tmpLocalSnapRaf = new RandomAccessFile(tmpLocalSnapFile, "rw");
            tmpLocalSnapRaf.writeInt(FILE_NOT_COMPLETE);
            tmpLocalSnapRaf.writeInt(LOCAL_FLAG);
            tmpLocalSnapRaf.writeLong( globalSeqNo );
            //
            // Copy all delta file to temp snapshot
            for (int i = 0; i < deltaFiles.size(); i++) {
                File deltaFile = deltaFiles.get(i);
                boolean isLastDeltaFile = i == deltaFiles.size() - 1;
                try {
                	globalSeqNo = this.copyToTmpFile(deltaFile, isLastDeltaFile, globalSeqNo, index, term, confState, tmpLocalSnapRaf);
                    LOGGER.info("copy delta {} to {} success !", deltaFile.getName(), tmpLocalSnapFile.getName());
                } catch (IOException e) {
                    LOGGER.error("copy delta err: {}", deltaFile.getName());
                    throw e;
                }
            }
            //
            tmpLocalSnapRaf.seek(0);
            tmpLocalSnapRaf.writeInt(FILE_COMPLETED);
            tmpLocalSnapRaf.writeInt(LOCAL_FLAG);
            tmpLocalSnapRaf.writeLong(globalSeqNo);
            //
            //
			File localSnapFile = new File(delegate.getLocalSnapDir(), delegate.toFileName(index, term));
			try {
				FileUtils.moveFile(tmpLocalSnapFile, localSnapFile);
			} catch (IOException e) {
				throw e;
			}
			//
			LOGGER.info("merge all delta to {} success !", localSnapFile.getName());

        } catch (IOException e) {
            LOGGER.error("merge delta err", e);
            throw new RaftException(e);
        } finally {
            IOUtil.closeQuietly(tmpLocalSnapRaf);
        }
    }

    //
	private long copyToTmpFile(File deltaFile, boolean isLastDeltaFile,
			long globalSeqNo, long index, long term,
			ConfState confState, RandomAccessFile tmpLocalSnapRaf) throws IOException {
       	//
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(deltaFile, "rw");
            raf.seek(8L);
            //
            long deltaSeqNo = raf.readLong();
            for (long j = 0; j < deltaSeqNo; j++) {
            	globalSeqNo++;
                //
                raf.readLong();// crc32
                int length = raf.readInt();
                //
                byte[] data = new byte[length];
                raf.read(data);
                //
                Snapshot.Builder snapshotBuilder = Snapshot.parseFrom(data).toBuilder();
                //
                // chunk
                // TODO: 修改seqNo、last
                Raftpb.SnapshotChunk chunk = snapshotBuilder.getChunk().toBuilder() //
                				.setSeqNo(globalSeqNo) //
                				.setLast(isLastDeltaFile && j == deltaSeqNo - 1) //
                				.build();
                //
                // meta
                // TODO: 修改 index、term、confState
                Raftpb.SnapshotMetadata metadata = snapshotBuilder.getMetadata().toBuilder() //
                		.setIndex(index) //
                		.setTerm(term) //
                		.setConfState( confState ) //
                		.build();
                //
                snapshotBuilder.setChunk( chunk );
                snapshotBuilder.setMetadata( metadata);

                data = snapshotBuilder.build().toByteArray();

                //
                // Restrict write throughput
                long writeBytes = 8 + 4 + data.length;
                writeBytes -= snapshotThrottle.throttledByThroughput(writeBytes);
                while (writeBytes > 0) {
                    long waitMilliSecs = writeBytes / throttleThroughputBytes * 1000L;
                    try {
                        Thread.sleep(waitMilliSecs > 0 ? waitMilliSecs : 1);
                    } catch (InterruptedException e) { }
                    writeBytes -= snapshotThrottle.throttledByThroughput(writeBytes);
                }

                // Write delta data to temporary files for local snapshots
                tmpLocalSnapRaf.writeLong(CrcUtil.crc32(data));
                tmpLocalSnapRaf.writeInt(data.length);
                tmpLocalSnapRaf.write(data);
            }

        } finally {
            IOUtil.closeQuietly(raf);
        }
        //
		return globalSeqNo;
    }

    //
    // ----------------------------------------------- delegate -------------------------------------------
    //
    @Override
    public void gc() {
        delegate.gc();
    }

    @Override
    public SnapshotMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public Writeable create(boolean isItRemote) {
        return delegate.create(isItRemote);
    }

    @Override
    public Readable open(boolean isItRemote, long index, long term) {
        return delegate.open(isItRemote, index, term);
    }

    @Override
    public void moveFile(Readable readable) {
        delegate.moveFile(readable);
    }
}
