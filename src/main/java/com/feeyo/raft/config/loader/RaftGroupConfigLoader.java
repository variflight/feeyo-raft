package com.feeyo.raft.config.loader;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Config;
import com.feeyo.raft.config.RaftGroupConfig;

public final class RaftGroupConfigLoader {
	//
	private static Logger LOGGER = LoggerFactory.getLogger(RaftGroupConfigLoader.class);
	//
	public static RaftGroupConfig load(String uri) {
		try {
			Map<String, String> propertyMap = ConfigLoader.getPropertyMapOfXml(uri);
			Config c = ConfigLoader.parseConfig(propertyMap);
			int tpCoreThreads = ConfigLoader.parseIntValue(propertyMap, "tpCoreThreads", 5);
			int tpMaxThreads = ConfigLoader.parseIntValue(propertyMap, "tpMaxThreads", 50);
			int tpQueueCapacity = ConfigLoader.parseIntValue(propertyMap, "tpQueueCapacity", 10);
			//
			RaftGroupConfig raftGroupConfig = new RaftGroupConfig(c);
			raftGroupConfig.setTpCoreThreads(tpCoreThreads);
			raftGroupConfig.setTpMaxThreads(tpMaxThreads);
			raftGroupConfig.setTpQueueCapacity(tpQueueCapacity);
			return raftGroupConfig;
			
		} catch (Exception e) {
			LOGGER.error("load raft group cfg err:", e);
		}
		return null;
	}

}
