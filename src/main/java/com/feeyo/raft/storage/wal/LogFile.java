package com.feeyo.raft.storage.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Errors;
import com.feeyo.raft.Errors.RaftException;
import com.feeyo.raft.proto.Raftpb.Entry;
import com.feeyo.raft.proto.Raftpb.HardState;
import com.feeyo.raft.storage.wal.proto.Walpb;
import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.MappedByteBufferUtil;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * LogFile
 */
public class LogFile extends AbstractLogFile  {
	
	private static Logger LOGGER = LoggerFactory.getLogger( LogFile.class );
	//
	//  MAGIC 
    public final static int START_MAGIC 	= 0xAABBCCDD ^ 1880681586 + 8;
    public final static int END_MAGIC 		= 0xBBCCDDEE ^ 1880681586 + 8;
    public final static int TRUNCATE_MAGIC 	= 0xCCDDEEFF ^ 1880681586 + 8;
    //
	public static final int START_LEN =  4 + 4 + 4;		// START_MAGIC + dataType + dataLength
	public static final int END_LEN = 4;				// END_MAGIC
	//
	protected AtomicBoolean isClosed = new AtomicBoolean( false );
	//
	protected FileChannel fc;
	protected MappedByteBuffer mappedByteBuffer;

    //当前写文件的位置
	protected final AtomicInteger wrotePos = new AtomicInteger(0);
	protected final AtomicInteger committedPos = new AtomicInteger(0);
    
	@SuppressWarnings("resource")
	public LogFile(String path, long firstLogIndex, long lastLogIndex, 
			int fileSize, boolean isWritable) throws IOException {
		this.path = path;
		this.firstLogIndex = firstLogIndex;
		this.lastLogIndex = lastLogIndex;
		this.fileName = AbstractLogFile.generateFileName(firstLogIndex, lastLogIndex);
		this.fileSize = fileSize;
		this.isWritable = isWritable;
		//
		boolean ok = false;
		try {
			this.file = new File(path, fileName);
			if ( !file.exists() )  {
				file.createNewFile();
				LOGGER.info("create new file {}",  fileName );
			}
			//
            this.fc = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fc.map(MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (Exception e) {
            LOGGER.error("new file err:" + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fc != null) {
            	IOUtil.closeQuietly(fc);
            }
        }
	}
	
	@Override
	public LogMetadata openAtIndex(long index) throws RaftException {	
		Walpb.Snapshot start = null;
		List<Entry> entries = new ArrayList<Entry>();
		HardState state = null;
		try {
			// read
			while ((fileSize - wrotePos.get()) > START_LEN + END_LEN) {
            	int startMagic = mappedByteBuffer.getInt();
            	int dataType = mappedByteBuffer.getInt();
            	int dataLength = mappedByteBuffer.getInt();
            	
            	// 数据完整性校验
            	// startMagicCode & record length 
				if (startMagic == START_MAGIC && ((fileSize - wrotePos.get() - START_LEN - dataLength - END_LEN) >= 0)) {
            		//
            		int position = mappedByteBuffer.position();
            		ByteBuffer data = MappedByteBufferUtil.slice(mappedByteBuffer, position, dataLength);
            		mappedByteBuffer.position( position + dataLength );
                	//
                	// endMagicCode
                	int endMagic = mappedByteBuffer.getInt();	
                	if ( endMagic == END_MAGIC ) {
                		int newPos =  mappedByteBuffer.position();
                		wrotePos.set( newPos );
                		//
                		if ( dataType == AbstractLogFile.DataType.ENTRY ) {
        					Entry entry = Entry.parseFrom(data);
        					if (entry == null) 
        						throw new Errors.RaftException(String.format("load file err, fileName=%s", fileName));
        					//
        					// 可写情况下，需要通过 openAtIndex 恢复 lastLogIndex 的值
							if (isWritable)
								lastLogIndex = entry.getIndex();
							//
							// skip entry
							if (index != -1 && entry.getIndex() <= index)
        						continue;
        					// 
        					entries.add( entry );
        					
        				} else if ( dataType == AbstractLogFile.DataType.STATE ) {
        					state = HardState.parseFrom(data);
        					
        				} else if ( dataType == AbstractLogFile.DataType.SNAPSHOT ) {
        					start = Walpb.Snapshot.parseFrom(data);
        				}               		
                	} else {
                		break;
                	}
            	} else {
            		break;
            	}
            }

		} catch (Throwable e) {
			throw new Errors.RaftException(String.format("load err: %s, size=%s, wrote=%s", fileName, fileSize, wrotePos), e);
		} 
		//
    	LOGGER.info("openAtIndex {}, name={}, wrotePos={}", index, fileName, wrotePos );
		return new LogMetadata(start, entries, state);
	}
	
	//
	// 剩余
	public int remainingBytes(int size) {
		if ( size > 0 )
			return fileSize - wrotePos.get() - size - 16;
		return  fileSize - wrotePos.get();
	}
	
	@Override
	public boolean append(int dataType, byte[] data) {
		int off = wrotePos.get();
		this.mappedByteBuffer.position(off);
		//
		// 确定是否有足够的空闲空间
		int dataLength = data.length;
		int recordLen = START_LEN + dataLength + END_LEN;
		//
		mappedByteBuffer.putInt(START_MAGIC);
		mappedByteBuffer.putInt(dataType);
		mappedByteBuffer.putInt(dataLength);
		mappedByteBuffer.put(data);
		mappedByteBuffer.putInt(END_MAGIC);
		//
		wrotePos.addAndGet(recordLen);
		return true;
	}
	

	@Override
	public boolean append(int dataType, ByteBuffer byteBuffer) {
		int off = wrotePos.get();
		this.mappedByteBuffer.position(off);
		//
		// 确定是否有足够的空闲空间
		int dataLength = byteBuffer.limit();
		int recordLen = START_LEN + dataLength + END_LEN;
		//
		mappedByteBuffer.putInt(START_MAGIC);
		mappedByteBuffer.putInt(dataType);
		mappedByteBuffer.putInt(dataLength);
		mappedByteBuffer.put( byteBuffer );
		mappedByteBuffer.putInt(END_MAGIC);
		//
		wrotePos.addAndGet(recordLen);
		return true;
	}
	
	
	@SuppressWarnings("resource")
	@Override
	public void truncate(long index) {
		try {
			//
			// 如果之前文件已关闭，此处需要重新打开
			if (isClosed.get()) {
				this.isWritable = true;
				this.lastLogIndex = 0;
				//
				String newFileName = AbstractLogFile.generateFileName(firstLogIndex, lastLogIndex);
				File newFile = new File(this.path, newFileName);
				com.google.common.io.Files.move(file, newFile); // rename with Google Guava
				//
				this.file = newFile;
				this.fileName = newFileName;

				// reopen
				try {
					this.fc = new RandomAccessFile(this.file, "rw").getChannel();
					this.mappedByteBuffer = this.fc.map(MapMode.READ_WRITE, 0, fileSize);
				} catch (Exception e) {
					LOGGER.error("reOpen err:" + this.fileName, e);
				}
			}

			//
			// reset
			wrotePos.set(0);
			int fileOffset = wrotePos.get();
			this.mappedByteBuffer.position(fileOffset);
			//
			// read
			while ((fileSize - wrotePos.get()) > START_LEN + END_LEN) {
				int startMagic = mappedByteBuffer.getInt();
				int dataType = mappedByteBuffer.getInt();
				int dataLength = mappedByteBuffer.getInt();
				//
				// 数据完整性校验
				//
				// startMagic & record length
				if (startMagic == START_MAGIC && ((fileSize - wrotePos.get() - START_LEN - dataLength - END_LEN) >= 0)) {
					//
            		int position = mappedByteBuffer.position();
            		ByteBuffer data = MappedByteBufferUtil.slice(mappedByteBuffer, position, dataLength);
            		mappedByteBuffer.position( position + dataLength );
					//
					// endMagicCode
					int endMagic = mappedByteBuffer.getInt();
					if (endMagic == END_MAGIC) {
						//
						int pos = mappedByteBuffer.position();
						wrotePos.set(pos);
						committedPos.set(pos);
						//
						if (dataType == AbstractLogFile.DataType.ENTRY) {
							try {
								Entry entry = Entry.parseFrom(data);
								if (entry != null) {
									if (entry.getIndex() == index) {
										// 强制插入 TRUNCATE_MAGIC
										mappedByteBuffer.putInt(TRUNCATE_MAGIC);
										break;
									}
								}
							} catch (InvalidProtocolBufferException e) {
								LOGGER.warn("parse err: name={}, size={}, wrotePos={}", fileName, fileSize, wrotePos);
							}
							//
						} else if (dataType == AbstractLogFile.DataType.STATE || dataType == AbstractLogFile.DataType.SNAPSHOT) {
							// skip
						}
					} else {
						break;
					}

				} else {
					break;
				}
			}
		} catch (IOException e) {
			LOGGER.error("truncate err:", e);
		}
	}
	
	@Override
	public void cut() {
		try {
			// close
			close();
			//
			String newFileName = AbstractLogFile.generateFileName(firstLogIndex, lastLogIndex);
			File newFile = new File(path, newFileName);
			com.google.common.io.Files.move(file, newFile);
			LOGGER.info("cut file: old={} to new={}", fileName, newFileName);
			//
			this.file = newFile;
			this.fileName = newFileName;
			//
		} catch (IOException e) {
			LOGGER.error("cut err:", e);
		}
	}
	
    //
 	public synchronized int flush() {
        boolean isFlush = wrotePos.get() > committedPos.get();
		if (isFlush) {
			int value = wrotePos.get();
             try {
                 // We only append data to fileChannel or mappedByteBuffer, never both.
					if (this.fc.position() != 0) {
						this.fc.force(false);
					} else {
                     this.mappedByteBuffer.force();
                 }
                 this.committedPos.set(value);
                 //
             } catch (Throwable e) {
                 LOGGER.error("Error occurred when force data to disk.", e);
             }
        }
        //
        return this.committedPos.get();
	}
	
	public void close() {
		if (!isClosed.compareAndSet(false, true))
			return;
		//
		isWritable = false;
		flush();
		MappedByteBufferUtil.clean(this.mappedByteBuffer);
		IOUtil.closeQuietly(fc);
	}

	@Override
	public void delete() {
		// close
		close();
		//
		try {
			FileUtils.forceDelete(file);
		} catch (IOException e) {
			LOGGER.info("delete file err: " + this.fileName, e);
		}
	}

	@Override
	public String toString() {
		return new StringBuffer()
		.append("[ ").append(fileName)
		.append(", wrotePos=").append(wrotePos)
		.append(", committedPos=").append(committedPos)
		.append(" ]")
		.toString();
	}
}