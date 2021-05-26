package com.feeyo.raft.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class FileBufferTest {
	
	
	public static void main(String[] args) throws IOException {
		
		String path = new File(".").getAbsolutePath();
		String baseDir =  path.substring(0, path.length() - 1);
		
		System.out.println( baseDir );
		
		
		byte[] bb = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 , 10 };
		
		
		FileBuffer fileBuffer = new FileBuffer( baseDir, 20 );
		
		for(int i=0; i < 10; i++) {
			fileBuffer.writeByteArray(bb);
		}
		
	
		byte[] bb1 = fileBuffer.readByteArray(50 - bb.length, bb.length);
		System.out.println( "bb1=" + Arrays.toString(bb1));
		
		byte[] bb2 = fileBuffer.readByteArray(60 - bb.length, bb.length);
		System.out.println( "bb2=" + Arrays.toString(bb2));
		
		//
		ByteBuffer bb3 = fileBuffer.readByteBuffer(50 - bb.length, bb.length);
		System.out.println( "bb3=" + bb3.limit());
		
		fileBuffer.compact(60);
		fileBuffer.clear();
		
	}

}
