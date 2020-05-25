package com.feeyo.raft.config.loader;

import com.feeyo.raft.Config;
import com.feeyo.raft.LinearizableReadOption;
import com.feeyo.raft.ReadOnlyOption;
import com.feeyo.raft.config.RaftConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.util.HashMap;
import java.util.Map;

public class RaftConfigLoader {
	//
	private static Logger LOGGER = LoggerFactory.getLogger(RaftConfigLoader.class);
	//
	//
	public static RaftConfig load(String uri) {
		try {

			Map<String, String> map = getPropertyMapOfXml( uri );
			//
			int electionTick = parseIntValue(map, "electionTick", 50); // 毫秒
			int heartbeatTick = parseIntValue(map, "heartbeatTick", 10);
			long applied = parseLongValue(map, "applied", 0);
			long maxSizePerMsg = parseLongValue(map, "maxSizePerMsg", 1024 * 1024); // 1MB
			int maxInflightMsgs = parseIntValue(map, "maxInflightMsgs", 256);
			int minElectionTick = parseIntValue(map, "minElectionTick", 0);
			int maxElectionTick = parseIntValue(map, "maxElectionTick", 0);
			int maxLogFileSize = parseIntValue(map, "maxLogFileSize", 50 * 1024 * 1024); // 50MB

			boolean checkQuorum = parseBooleanValue(map, "checkQuorum", true);
			boolean preVote = parseBooleanValue(map, "preVote", true);
			boolean skipBcastCommit = parseBooleanValue(map, "skipBcastCommit", false);
			boolean disableProposalForwarding = parseBooleanValue(map, "disableProposalForwarding", false);
			long snapCount = parseLongValue(map, "snapCount", 10000);				// 10000 Records
			int snapInterval = parseIntValue(map, "snapInterval", 45 * 60 * 1000); // 45 Minutes 

			boolean syncLog = parseBooleanValue(map, "syncLog", false);
			//
			int tpCoreThreads = parseIntValue(map, "tpCoreThreads", 5);
			int tpMaxThreads = parseIntValue(map, "tpMaxThreads", 50);
			int tpQueueCapacity = parseIntValue(map, "tpQueueCapacity", 10);

			String storageDir = map.get("storageDir");
			ReadOnlyOption readOnlyOption = ReadOnlyOption.fromString(map.get("readOnlyOption"));
			LinearizableReadOption linearizableReadOption = LinearizableReadOption.fromString(map.get("linearizableReadOption"));
			
			//
			Config c = new Config();
			c.setElectionTick(electionTick);
			c.setHeartbeatTick(heartbeatTick);
			c.setApplied(applied);
			c.setMaxSizePerMsg(maxSizePerMsg);
			c.setMaxInflightMsgs(maxInflightMsgs);
			c.setMinElectionTick(minElectionTick);
			c.setMaxElectionTick(maxElectionTick);
			c.setMaxLogFileSize(maxLogFileSize);
			c.setSnapCount(snapCount);
			c.setSnapInterval(snapInterval);
			c.setCheckQuorum(checkQuorum);
			c.setPreVote(preVote);
			c.setSkipBcastCommit(skipBcastCommit);
			c.setStorageDir(storageDir);
			c.setReadOnlyOption(readOnlyOption);
			c.setLinearizableReadOption(linearizableReadOption);
			c.setDisableProposalForwarding(disableProposalForwarding);
			c.setSyncLog(syncLog);
			//
			RaftConfig cfg = new RaftConfig(c);
			cfg.setTpCoreThreads(tpCoreThreads);
			cfg.setTpMaxThreads(tpMaxThreads);
			cfg.setTpQueueCapacity(tpQueueCapacity);

			return cfg;
		} catch (Exception e) {
			LOGGER.error("load raft cfg err:", e);
		}
		return null;
	}
	
	private static Map<String, String> getPropertyMapOfXml(String uri) throws Exception {
		
		Map<String, String> map = new HashMap<String, String>();
		//
		NodeList propertyNodes = loadXmlDoc(uri).getElementsByTagName("property");
		for (int i = 0; i < propertyNodes.getLength(); i++) {
			Node node = propertyNodes.item(i);
			if (node instanceof Element) {
				Element e = (Element) node;
				String key = e.getAttribute("name");
				String value = e.getTextContent();
				map.put(key, value);
			}
		}
		
		return map;
	}

	private static Document loadXmlDoc(String uri) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(uri);
		return doc;
	}
	
	private static boolean parseBooleanValue(Map<String, String> map, String key, boolean defaultValue) {
		String value = map.get(key);
		if (value == null)
			return defaultValue;
		return Boolean.parseBoolean(value);
	}

	private static int parseIntValue(Map<String, String> map, String key, int defaultValue) {
		String value = map.get(key);
		if (value == null)
			return defaultValue;
		return Integer.parseInt(value);
	}

	private static long parseLongValue(Map<String, String> map, String key, long defaultValue) {
		String value = map.get(key);
		if (value == null)
			return defaultValue;
		return Long.parseLong(value);

	}
}