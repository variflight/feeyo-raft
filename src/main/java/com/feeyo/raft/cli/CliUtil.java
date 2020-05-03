package com.feeyo.raft.cli;

import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.feeyo.raft.cli.util.TextUtil;
import com.feeyo.util.internal.Utf8Util;

public class CliUtil {
	
	
	public static String addNode(String hostAndPort, 
			String id, String ip, String port, String isLearner) throws CliException {

		try {

			String url = String.format("http://%s/raft/cli?cmd=addNode&id=%s&ip=%s&port=%s&isLearner=%s", 
					hostAndPort, id, ip, port, isLearner);
			
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				String[] columnNames = { "code", "data" };
				Object[][] data = new Object[1][2];
				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(result.content) );
				data[0][0] = jsonObject1.getIntValue("code");
				data[0][1] = jsonObject1.getString("data");
				
				return TextUtil.toText(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return TextUtil.toText(columnNames, data);
			}
			
		} catch (IOException e) {
			throw new CliException("http err", e);
			
		} 
		
	}
	
	public static String removeNode(String hostAndPort, String id) throws CliException {
		
		try {
			String url = String.format("http://%s/raft/cli?cmd=removeNode&id=%s", hostAndPort, id);
			
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				String[] columnNames = { "code", "data" };
				Object[][] data = new Object[1][2];
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(result.content) );
				data[0][0] = jsonObject1.getIntValue("code");
				data[0][1] = jsonObject1.getString("data");
				
				return TextUtil.toText(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return TextUtil.toText(columnNames, data);
			}

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	
	public static String transferLeader(String hostAndPort, String id) throws CliException {
		
		try {
			
			String url = String.format("http://%s/raft/cli?cmd=transferLeader&id=%s", hostAndPort, id);
			
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				String[] columnNames = { "code", "data" };
				Object[][] data = new Object[1][2];
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( result.content ) );
				data[0][0] = jsonObject1.getIntValue("code");
				data[0][1] = jsonObject1.getString("data");
				
				return TextUtil.toText(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return TextUtil.toText(columnNames, data);
			}

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	//
	public static String getNodes(String hostAndPort) throws CliException {
		
		try {
			
			String url = String.format("http://%s/raft/cli?cmd=getNodes", hostAndPort);
	
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( result.content ) );
				JSONArray jsonArray1 = jsonObject1.getJSONArray("data");

				String[] columnNames = { "id", "ip", "port", "state" };
				Object[][] data = new Object[ jsonArray1.size() ][4];
				for(int i = 0; i < jsonArray1.size(); i++){
					JSONObject jsonObject2 = jsonArray1.getJSONObject(i);
					data[i][0] = jsonObject2.getLongValue("id");
					data[i][1] = jsonObject2.getString("ip");
					data[i][2] = jsonObject2.getIntValue("port");
					data[i][3] = jsonObject2.getString("state");
				}
				return TextUtil.toText(columnNames, data);
				
			} else {
				
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return TextUtil.toText(columnNames, data);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new CliException("http err", e);
			
		} 
	}
	
	
	public static String getNodePrs(String hostAndPort) throws CliException {
		
		try {
			
			String url = String.format("http://%s/raft/cli?cmd=getNodePrs", hostAndPort);
			
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( result.content ) );
				JSONArray jsonArray1 = jsonObject1.getJSONArray("data");
				
				String[] columnNames = { "id", "matched", "nextIndex", "pendingSnapshot", "isRecentActive", "isLearner", "isPaused", "state" };
				Object[][] columnData = new Object[ jsonArray1.size() ][8];
				for(int i = 0; i < jsonArray1.size(); i++){
					
					JSONObject jsonObject2 = jsonArray1.getJSONObject(i);
					//
					columnData[i][0] = jsonObject2.getLongValue("id");
					columnData[i][1] = jsonObject2.getLongValue("matched");
					columnData[i][2] = jsonObject2.getLongValue("nextIndex");
					columnData[i][3] = jsonObject2.getLongValue("pendingSnapshot");
					columnData[i][4] = jsonObject2.getBoolean("isRecentActive");
					columnData[i][5] = jsonObject2.getBoolean("isLearner");
					columnData[i][6] = jsonObject2.getBoolean("isPaused");
					columnData[i][7] = jsonObject2.getString("state");
				}
				return TextUtil.toText(columnNames, columnData);
				
			} else {
				
				String[] columnNames = { "code", "content" };
				Object[][] columnData = new Object[1][2];
				columnData[0][0] = result.code;
				columnData[0][1] = Utf8Util.readUtf8( result.content );
				return TextUtil.toText(columnNames, columnData);
			}
			
		} catch (IOException e) {
			throw new CliException("http err", e);
			
		} 
	}
	
}
