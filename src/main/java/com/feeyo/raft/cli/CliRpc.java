package com.feeyo.raft.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.feeyo.util.internal.Utf8Util;

public class CliRpc {
	//
	public static class ScreenFmtData {
		List<List<String>> lists;
		List<Integer> maxSizeList;
		public ScreenFmtData(List<List<String>> lists, List<Integer> maxSizeList) {
			this.lists = lists;
			this.maxSizeList = maxSizeList;
		}
	}
	//
	private static ScreenFmtData toScreenFmtData(String[] columnNames, Object[][] data) {
		List<List<String>> lists = new ArrayList<>();
		List<Integer> maxSizeList = new ArrayList<>();
		//
		for (int i = 0; i < columnNames.length; i++) {
			List<String> column_list = new ArrayList<>();
			column_list.add( columnNames[i] );
			lists.add(column_list);
			maxSizeList.add( columnNames[i].length() );
		}
		//
		for(int i = 0; i < data.length; i++) {
			for(int j = 0; j < data[i].length; j++) {
				String v = "";
				if ( v != null ) {
					if (data[i][j] instanceof Integer || data[i][j] instanceof Long) {
						v = String.valueOf( (long)data[i][j] );
						
					} else if (data[i][j] instanceof Double) {
							v = String.valueOf( (double)data[i][j] );
							
					} else if (data[i][j] instanceof Float) {
						v = String.valueOf( (float)data[i][j] );
						
					} else if (data[i][j] instanceof Boolean) {
						v = String.valueOf( (boolean)data[i][j]);
					} else {
						v = String.valueOf( data[i][j] );
					}
				}
				//
				maxSizeList.set(i, Math.max(maxSizeList.get(i), v.length()) );
				lists.get(i).add(v);
			}
		}
		return new ScreenFmtData(lists, maxSizeList);
	}
	
	//
	public static ScreenFmtData addNode(String hostAndPort, 
			String id, String ip, String port, String isLearner) throws CliException {
		//
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
				
				for(int i = 0; i < columnNames.length; i++) {
					jsonObject1.get( columnNames[i] );
				}
				
				return toScreenFmtData(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return toScreenFmtData(columnNames, data);
			}
			
			
		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
		
	}
	
	public static ScreenFmtData removeNode(String hostAndPort, String id) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=removeNode&id=%s", hostAndPort, id);
			//
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				String[] columnNames = { "code", "data" };
				Object[][] data = new Object[1][2];
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8(result.content) );
				data[0][0] = jsonObject1.getIntValue("code");
				data[0][1] = jsonObject1.getString("data");
				
				return toScreenFmtData(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return toScreenFmtData(columnNames, data);
			}

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	
	public static ScreenFmtData transferLeader(String hostAndPort, String id) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=transferLeader&id=%s", hostAndPort, id);
			//
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				String[] columnNames = { "code", "data" };
				Object[][] data = new Object[1][2];
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( result.content ) );
				data[0][0] = jsonObject1.getIntValue("code");
				data[0][1] = jsonObject1.getString("data");
				
				return toScreenFmtData(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return toScreenFmtData(columnNames, data);
			}

		} catch (IOException e) {
			throw new CliException("http err", e);
		} 
	}
	
	//
	public static ScreenFmtData getNodes(String hostAndPort) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=getNodes", hostAndPort);
			//
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
				return toScreenFmtData(columnNames, data);
				
			} else {
				String[] columnNames = { "code", "content" };
				Object[][] data = new Object[1][2];
				data[0][0] = result.code;
				data[0][1] = new String( result.content );
				return toScreenFmtData(columnNames, data);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new CliException("http err", e);
			
		} 
	}
	
	
	public static ScreenFmtData getNodePrs(String hostAndPort) throws CliException {
		try {
			String url = String.format("http://%s/raft/cli?cmd=getNodePrs", hostAndPort);
			//
			HttpURLConnectionUtil.Result result = HttpURLConnectionUtil.tryGet( url, null);
			if ( result != null && result.code == 200 ) {
				
				JSONObject jsonObject1 = JSON.parseObject( Utf8Util.readUtf8( result.content ) );
				JSONArray jsonArray1 = jsonObject1.getJSONArray("data");
				
				String[] columnNames = { "id", "matched", "nextIndex", "pendingSnapshot", "isRecentActive", "isLearner", "isPaused", "state" };
				Object[][] columnData = new Object[ jsonArray1.size() ][8];
				for(int i = 0; i < jsonArray1.size(); i++){
					
					JSONObject jsonObject2 = jsonArray1.getJSONObject(i);
					columnData[i][0] = jsonObject2.getLongValue("id");
					columnData[i][1] = jsonObject2.getLongValue("matched");
					columnData[i][2] = jsonObject2.getLongValue("nextIndex");
					columnData[i][3] = jsonObject2.getLongValue("pendingSnapshot");
					columnData[i][4] = jsonObject2.getBoolean("isRecentActive");
					columnData[i][5] = jsonObject2.getBoolean("isLearner");
					columnData[i][6] = jsonObject2.getBoolean("isPaused");
					columnData[i][7] = jsonObject2.getString("state");
				}
				return toScreenFmtData(columnNames, columnData);
				
			} else {
				
				String[] columnNames = { "code", "content" };
				Object[][] columnData = new Object[1][2];
				columnData[0][0] = result.code;
				columnData[0][1] = Utf8Util.readUtf8( result.content );
				return toScreenFmtData(columnNames, columnData);
			}
			
		} catch (IOException e) {
			throw new CliException("http err", e);
			
		} 
	}
}