package com.feeyo.raft.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.feeyo.net.codec.UnknownProtocolException;
import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpRequestPipeliningDecoder;
import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.raft.proto.Raftpb.Message;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class HttpRequestPipeliningDecoderTest2 {
	
	//
	private static ProtobufDecoder<Message> protobufDecoder 
						= new ProtobufDecoder<Message>(Message.getDefaultInstance(), true);
	
	public static void main(String[] args) throws UnknownProtocolException, IOException {
		
		// 5701160-70183.txt
		File file = new File("/Users/zhuam/Downloads/xx1/5701160-70183.txt");
		ByteSource source = Files.asByteSource(file);

		byte[] result = source.read();
		
		
		HttpRequestPipeliningDecoder decoder = new HttpRequestPipeliningDecoder();
		decoder.setReadOffset(0);
		
		int count = 0;
		
		for(int i = 0; i< result.length; i++) {
			byte[] bb = new byte[] { result[i] };
			List<HttpRequest> rr = decoder.decode( bb );
			if ( rr != null ) {
				//
				for(HttpRequest r: rr) {
					
					count++;
					
					List<Message> msgs = protobufDecoder.decode( r.getContent() );
					
					System.out.println(  "contentLength=" +  r.getContent().length + ", msgSize=" + msgs.size() + ", msgType=" + msgs.get(0).getMsgType() );
				}
			}
		}
		
		
		System.out.println( "-------------------------" );
		System.out.println( count );
		System.out.println( decoder.getBufferSize() + ", " + decoder.getReadOffset());

	}

}
