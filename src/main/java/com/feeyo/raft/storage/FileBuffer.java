package com.feeyo.raft.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.util.IOUtil;
import com.feeyo.raft.util.MappedByteBufferUtil;
import com.google.common.base.Joiner;

/**
 * Non thread-safe
 * 
 * @author zhuam
 *
 */
public class FileBuffer {
	//
	private static Logger LOGGER = LoggerFactory.getLogger( FileBuffer.class );
	//
	private static final String fileSuffix = "segment";
	private static final Joiner fileNameJoiner = Joiner.on(".");
	private static final Joiner filePathJoiner = Joiner.on( File.separator );
	private static final int MAX_SIZE = 1024 * 1024 * 64; 		 // 64MB
	//
	private final String baseDir;
	private final int size;
	//
	private long lastWrotePos = 0;
	//
	private List<SegmentFile> segementFiles = new CopyOnWriteArrayList<SegmentFile>();
	
	static {
		//
		// @see https://bugs.openjdk.java.net/browse/JDK-8175970
		// @see https://blogs.oracle.com/poonam/crashes-in-zipgetentry
		System.setProperty("sun.zip.disableMemoryMapping", "true");
		//
	}

	public FileBuffer(String baseDir) {
		this(baseDir, MAX_SIZE); 
	}
	//
	public FileBuffer(String baseDir, int size) {
		this.baseDir = baseDir;
		this.size = size;
		//
		File f = new File( baseDir );
		if (!f.exists()) {
			try {
				FileUtils.forceMkdir(f);
			} catch (IOException e) {
				LOGGER.error("Fail to create segment directory: {}", this.baseDir, e);
			}
		} else {
			// 删除该目录下所有文件文件
			if ( f.isDirectory() ) {
				Collection<File> files = FileUtils.listFiles(f, new String[]{ fileSuffix } , false);
				for(File f2: files) {
					try {
						FileUtils.forceDelete( f2 );
					} catch (IOException e) {
						LOGGER.warn("Fail to delete " + f2.getName(), e);
					}
				}
			}
		}
	}
	//
	private SegmentFile findSegmentFileByPosition(long position) {
		for (int i = segementFiles.size() - 1; i >= 0; i--) {
			SegmentFile file = segementFiles.get(i);
			if (position >= file.startPos && position < file.endPos) {
				if (file.isClosed)
					throw new IllegalArgumentException("read err, segment file is closed, name=" + file.fileName);
				return file;
			}
		}
		throw new IllegalArgumentException("read err, segment file not found at " + position );
	}
	//
	private SegmentFile getLastSegmentFile(int waitToWroteSize) throws IOException {
		SegmentFile file = null;
		if ( !segementFiles.isEmpty() ) {
			SegmentFile tmpFile = this.segementFiles.get(this.segementFiles.size() - 1);
			if ( waitToWroteSize <= tmpFile.remaining() ) 
				file = tmpFile;
		}
		if (file == null) {
			file = new SegmentFile(baseDir, lastWrotePos, size);
			segementFiles.add(file);
		}
		//
		if (file.isClosed)
			throw new IllegalArgumentException("write err, segment file is closed, name=" + file.fileName);
		return file;
	}
	//
	private void ensureMaxCapacity(int waitToWroteSize) {
		if (waitToWroteSize > size)
			throw new IllegalArgumentException("write err, exceeding the capacity of segment files, dataSize=" + waitToWroteSize);
	}
	//
	public long writeByteBuffer(ByteBuffer byteBuffer) throws IOException {
		int waitToWroteSize = byteBuffer.limit();
		ensureMaxCapacity( waitToWroteSize );
		//
		SegmentFile lastFile = getLastSegmentFile(waitToWroteSize);
		lastWrotePos = lastFile.writeByteBuffer( byteBuffer );
		return lastWrotePos;
	}
	//
	public long writeByteArray(byte[] byteArray) throws IOException {
		int waitToWroteSize = byteArray.length;
		ensureMaxCapacity( waitToWroteSize );
		//
		SegmentFile lastFile = getLastSegmentFile( waitToWroteSize );
		lastWrotePos = lastFile.writeByteArray(byteArray);
		return lastWrotePos;
	}
	//
	public byte[] readByteArray(long position, int length) {
		SegmentFile file = findSegmentFileByPosition(position);
		return file.readByteArray(position, length);
	}
	//
	public ByteBuffer readByteBuffer(long position, int length) {
		SegmentFile file = findSegmentFileByPosition(position);
		return file.readByteBuffer(position, length);
	}
	//
	public void compact(long position) {
		Iterator<SegmentFile> it = segementFiles.iterator();
		while (it.hasNext()) {
			SegmentFile file = it.next();
			if (file.endPos <= position) {
				try {
					file.delete();
					LOGGER.info("delete {}", file.fileName);
				} catch (IOException e) {
					LOGGER.warn("delete " + file.fileName + " err", e);
				}
				segementFiles.remove(file);
			}
		}
		LOGGER.info("Compact the file buffer from the {} position.", position);
	}
	//
	public void clear() {
		Iterator<SegmentFile> it = segementFiles.iterator();
		while (it.hasNext()) {
			SegmentFile file = it.next();
			if ( file != null ) {
				try {
					file.delete();
					LOGGER.info("delete {}", file.fileName);
				} catch (IOException e) {
					LOGGER.warn("delete " + file.fileName + " err", e);
				}
			}
		}
		segementFiles.clear();
		lastWrotePos = 0;
	}
	//
	public void close() {
		Iterator<SegmentFile> it = segementFiles.iterator();
		while (it.hasNext()) {
			SegmentFile file = it.next();
			if (file != null)
				file.close();
		}
	}
	
	/**
	 * 基于文件的缓存
	 */
	static class SegmentFile {
		private String fileName;
		private long startPos;
		private long endPos;
		//
		private final File file;
		private final RandomAccessFile raf;
		private final FileChannel fc;
		private final MappedByteBuffer mappedByteBuffer;
		private int fileSize;
		private volatile boolean isClosed = false;
		//
		// TODO: 避免readByteBuffer的线程安全问题，因为slice里面存在修改position的问题
		private Object _lock = new Object();
		//
		public SegmentFile(String baseDir, long startPos, int size) throws IOException {
			this.startPos = startPos;
			this.endPos = startPos;
			this.fileSize = size;
			this.fileName = fileNameJoiner.join(startPos, fileSuffix);
			this.file = new File(filePathJoiner.join(baseDir, fileName));
			this.raf = new RandomAccessFile(file, "rw");
			this.raf.setLength(fileSize);
			this.raf.seek(0);
			this.fc = raf.getChannel();
			this.mappedByteBuffer = fc.map(MapMode.READ_WRITE, 0, fc.size());
		}
		//
		public long writeByteBuffer(ByteBuffer byteBuffer) {
			synchronized (_lock) {
				mappedByteBuffer.put(byteBuffer);
				endPos = startPos + mappedByteBuffer.position();
				return endPos;
			}
		}
		//
		public long writeByteArray(byte[] byteArray) {
			synchronized (_lock) {
				mappedByteBuffer.put(byteArray);
				endPos = startPos + mappedByteBuffer.position();
				return endPos;
			}
		}
		//
		public byte[] readByteArray(long position, int length) {
			int pos = (int)(position - startPos);
			byte[] byteArray = new byte[length];
			for (int i = 0; i < length; i++) 
				byteArray[i] = mappedByteBuffer.get(pos + i);
			return byteArray;
		}
		
		public ByteBuffer readByteBuffer(long position, int length) {
			synchronized (_lock) {
				int pos = (int)(position - startPos);
				return MappedByteBufferUtil.slice(mappedByteBuffer, pos, length);
			}
		}
		
		public int remaining() {
			return mappedByteBuffer.remaining();
		}

		public synchronized void close() {
			if (isClosed)
				return;
			isClosed = true;
			MappedByteBufferUtil.clean(mappedByteBuffer);
			IOUtil.closeQuietly(raf);
			IOUtil.closeQuietly(fc);
		}
		//
		public void delete() throws IOException  {
			close();
			FileUtils.forceDelete(file);
		}
	}
}