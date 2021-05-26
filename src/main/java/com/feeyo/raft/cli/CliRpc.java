package com.feeyo.raft.cli;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.feeyo.util.internal.Utf8Util;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

public class CliRpc {
	//
    private static HttpResponse tryHttpGet(String uri, byte[] body) throws IOException  {
    	HttpResponse response = null;
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
            response = new HttpResponse();
            response.code = connection.getResponseCode();
            response.content = ByteStreams.toByteArray(connection.getInputStream());
            //
            connection.getInputStream().close();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return response;
    }
    //
    private static class HttpResponse {
    	public int code;
    	public byte[] content;
    }
	
	//
	public static Object[] addNode(String hostAndPort, 
			String id, String ip, String port, String isLearner) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=addNode&id=%s&ip=%s&port=%s&isLearner=%s", 
					hostAndPort, id, ip, port, isLearner);
			//
			List<List<String>> lists = new ArrayList<>();
			List<Integer> maxSizeList = new ArrayList<>();
			//
			HttpResponse response = tryHttpGet( url, null);
			if ( response != null && response.code == 200 ) {
				//
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(response.content) );
				lists.add( Lists.newArrayList("code", String.valueOf(jsonObject1.getIntValue("code"))) );
				lists.add( Lists.newArrayList("data", jsonObject1.getString("data")) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
				
			} else {
				//
				lists.add( Lists.newArrayList("code", String.valueOf( response.code )) );
				lists.add( Lists.newArrayList("content", new String( response.content )) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			}
			//
			return new Object[]{lists, maxSizeList};

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	public static Object[] removeNode(String hostAndPort, String id) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=removeNode&id=%s", hostAndPort, id);
			//
			List<List<String>> lists = new ArrayList<>();
			List<Integer> maxSizeList = new ArrayList<>();
			//
			HttpResponse response = tryHttpGet( url, null);
			if ( response != null && response.code == 200 ) {
				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(response.content) );
				lists.add( Lists.newArrayList("code", String.valueOf(jsonObject1.getIntValue("code"))) );
				lists.add( Lists.newArrayList("data", jsonObject1.getString("data")) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			} else {
				//
				lists.add( Lists.newArrayList("code", String.valueOf( response.code )) );
				lists.add( Lists.newArrayList("content", new String( response.content )) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			}
			//
			return new Object[]{lists, maxSizeList};

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	
	public static Object[] transferLeader(String hostAndPort, String id) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=transferLeader&id=%s", hostAndPort, id);
			//
			List<List<String>> lists = new ArrayList<>();
			List<Integer> maxSizeList = new ArrayList<>();
			//
			HttpResponse response = tryHttpGet( url, null);
			if ( response != null && response.code == 200 ) {
				//				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(response.content) );
				lists.add( Lists.newArrayList("code", String.valueOf(jsonObject1.getIntValue("code"))) );
				lists.add( Lists.newArrayList("data", jsonObject1.getString("data")) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}

			} else {
				//
				lists.add( Lists.newArrayList("code", String.valueOf( response.code )) );
				lists.add( Lists.newArrayList("content", new String( response.content )) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			}
			//
			return new Object[]{lists, maxSizeList};

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	//
	public static Object[] getNodes(String hostAndPort) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=getNodes", hostAndPort);
			//
			List<List<String>> lists = new ArrayList<>();
			List<Integer> maxSizeList = new ArrayList<>();
			//
			HttpResponse response = tryHttpGet( url, null);
			if ( response != null && response.code == 200 ) {
				//
				lists.add( Lists.newArrayList("id") );
				lists.add( Lists.newArrayList("ip") );
				lists.add( Lists.newArrayList("port") );
				lists.add( Lists.newArrayList("state") );
				//
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( response.content ) );
				JSONArray jsonArray1 = jsonObject1.getJSONArray("data");
				for(int i = 0; i < jsonArray1.size(); i++){
					JSONObject jsonObject2 = jsonArray1.getJSONObject(i);
					lists.get(0).add( String.valueOf(jsonObject2.getLongValue("id")) );
					lists.get(1).add( jsonObject2.getString("ip") );
					lists.get(2).add( String.valueOf(jsonObject2.getIntValue("port")) );
					lists.get(3).add( jsonObject2.getString("state") );
				}
				//
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
				
			} else {
				//
				lists.add( Lists.newArrayList("code", String.valueOf( response.code )) );
				lists.add( Lists.newArrayList("content", new String( response.content )) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			}
			//
			return new Object[]{lists, maxSizeList};
			
		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	
	public static Object[] getNodePrs(String hostAndPort) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=getNodePrs", hostAndPort);
			//
			List<List<String>> lists = new ArrayList<>();
			List<Integer> maxSizeList = new ArrayList<>();
			//
			HttpResponse response = tryHttpGet( url, null);
			if ( response != null && response.code == 200 ) {
				//
				lists.add( Lists.newArrayList("id") );
				lists.add( Lists.newArrayList("matched") );
				lists.add( Lists.newArrayList("nextIndex") );
				lists.add( Lists.newArrayList("pendingSnapshot") );
				lists.add( Lists.newArrayList("isRecentActive") );
				lists.add( Lists.newArrayList("isLearner") );
				lists.add( Lists.newArrayList("isPaused") );
				lists.add( Lists.newArrayList("state") );
				//
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( response.content ) );
				JSONArray jsonArray1 = jsonObject1.getJSONArray("data");
				for(int i = 0; i < jsonArray1.size(); i++){
					JSONObject jsonObject2 = jsonArray1.getJSONObject(i);
					lists.get(0).add( String.valueOf(jsonObject2.getLongValue("id")) );
					lists.get(1).add( String.valueOf(jsonObject2.getLongValue("matched")) );
					lists.get(2).add( String.valueOf(jsonObject2.getLongValue("nextIndex")) );
					lists.get(3).add( String.valueOf(jsonObject2.getLongValue("pendingSnapshot")) );
					lists.get(4).add( String.valueOf(jsonObject2.getBoolean("isRecentActive")) );
					lists.get(5).add( String.valueOf(jsonObject2.getBoolean("isLearner")) );
					lists.get(6).add( String.valueOf(jsonObject2.getBoolean("isPaused")) );
					lists.get(7).add( jsonObject2.getString("state") );
				}
				//
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
				
			} else {
				//
				lists.add( Lists.newArrayList("code", String.valueOf( response.code )) );
				lists.add( Lists.newArrayList("content", new String( response.content )) );
				for(int i = 0; i < lists.size(); i++) { 
					int maxSize = 0;
					for(String str: lists.get(i)) {
						maxSize = Math.max(maxSize, str.length());
					}
					maxSizeList.add(maxSize);
				}
			}
			//
			return new Object[]{lists, maxSizeList};
			
		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
}