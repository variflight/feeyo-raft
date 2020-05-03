package com.feeyo.raft.util.metric;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.feeyo.metrics.Gauge;
import com.feeyo.metrics.Metric;
import com.feeyo.metrics.MetricName;
import com.feeyo.metrics.MetricSet;

public class GaugeSet implements MetricSet {
	
	//
	private volatile long committed;
	private volatile long applied;
	
	private Map<String, Metric> gauges = new HashMap<String, Metric>();
	
	public GaugeSet() {
		//
		gauges.put(MetricName.name("raft.commmitted"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return committed;
            }
        });

        gauges.put(MetricName.name("raft.applied"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return applied;
            }
        });
	}
	
	public void update(long committed, long applied) {
		this.committed = committed;
		this.applied = applied;        
	}
	
	@Override
	public Map<String, Metric> getMetrics() {
		return Collections.unmodifiableMap(gauges);
	}

}
