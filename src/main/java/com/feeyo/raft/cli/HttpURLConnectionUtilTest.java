package com.feeyo.raft.cli;

import java.io.IOException;

public class HttpURLConnectionUtilTest {
	
	//
	public static void main(String[] args) {
		
		try {
			HttpURLConnectionUtil.tryGet("http://127.0.0.1:8080/raft/ping", null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
