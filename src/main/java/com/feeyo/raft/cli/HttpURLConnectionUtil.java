package com.feeyo.raft.cli;

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

class HttpURLConnectionUtil {

    public static Result tryGet(String uri, byte[] body) throws IOException  {
    	//
    	Result result = null;
        HttpURLConnection connection = null;
        try {
            URL url = new URL(uri);
            connection = (HttpURLConnection) url.openConnection();
            connection.setInstanceFollowRedirects(false);
            connection.setReadTimeout(3000);
            connection.setConnectTimeout(3000);
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setFixedLengthStreamingMode( body != null ? body.length : 0);
            if ( body != null ) {
            	connection.getOutputStream().write(body, 0, body.length);
                connection.getOutputStream().flush();
                connection.getOutputStream().close();
            } 

            result = new Result();
            result.code = connection.getResponseCode();
            result.content = ByteStreams.toByteArray(connection.getInputStream());
            //
            connection.getInputStream().close();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return result;
    }
    //
    public static class Result {
    	public int code;
    	public byte[] content;
    }
    
    ///
    //
    public static void main(String[] args) {
    	try {
			HttpURLConnectionUtil.tryGet("http://127.0.0.1:8080/raft/cli?cmd=getNodes", null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}