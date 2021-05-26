package com.feeyo.raft.config.loader;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.raft.Config;
import com.feeyo.raft.config.RaftGroupConfig;
import com.feeyo.raft.group.proto.Raftgrouppb.Region;

public final class RaftGroupConfigLoader {
	//
	private static Logger LOGGER = LoggerFactory.getLogger(RaftGroupConfigLoader.class);
	//
	public static RaftGroupConfig load(String raftUri, String regionUri) {
		try {
			Map<String, String> propertyMap = ConfigLoader.getPropertyMapOfXml(raftUri);
			Config c = ConfigLoader.parseConfig(propertyMap);
			int tpCoreThreads = ConfigLoader.parseIntValue(propertyMap, "tpCoreThreads", 5);
			int tpMaxThreads = ConfigLoader.parseIntValue(propertyMap, "tpMaxThreads", 50);
			int tpQueueCapacity = ConfigLoader.parseIntValue(propertyMap, "tpQueueCapacity", 10);
			//
			RaftGroupConfig raftGroupConfig = new RaftGroupConfig(c);
			raftGroupConfig.setTpCoreThreads(tpCoreThreads);
			raftGroupConfig.setTpMaxThreads(tpMaxThreads);
			raftGroupConfig.setTpQueueCapacity(tpQueueCapacity);
			//
			List<Region> regions = RegionLoader.load(regionUri);
			raftGroupConfig.setRegions(regions);
			return raftGroupConfig;
			
		} catch (Exception e) {
			LOGGER.error("load raft group cfg err:", e);
		}
		return null;
	}
	
	

}
