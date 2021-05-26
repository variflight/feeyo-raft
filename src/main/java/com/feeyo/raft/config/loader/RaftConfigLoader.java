package com.feeyo.raft.config.loader;

import com.feeyo.raft.Config;
import com.feeyo.raft.config.RaftConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;


public final class RaftConfigLoader {
	//
	private static Logger LOGGER = LoggerFactory.getLogger(RaftConfigLoader.class);
	//
	public static RaftConfig load(String raftUri) {
		try {
			Map<String, String> propertyMap = ConfigLoader.getPropertyMapOfXml(raftUri);
			Config c = ConfigLoader.parseConfig(propertyMap);
			int tpCoreThreads = ConfigLoader.parseIntValue(propertyMap, "tpCoreThreads", 5);
			int tpMaxThreads = ConfigLoader.parseIntValue(propertyMap, "tpMaxThreads", 50);
			int tpQueueCapacity = ConfigLoader.parseIntValue(propertyMap, "tpQueueCapacity", 10);
			//
			RaftConfig raftConfig = new RaftConfig(c);
			raftConfig.setTpCoreThreads(tpCoreThreads);
			raftConfig.setTpMaxThreads(tpMaxThreads);
			raftConfig.setTpQueueCapacity(tpQueueCapacity);
			return raftConfig;
		} catch (Exception e) {
			LOGGER.error("load raft cfg err:", e);
		}
		return null;
	}
}