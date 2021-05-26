package com.feeyo.raft.util.metric;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.feeyo.metrics.Histogram;

public class HistogramSet {
	
	private Map<String, Histogram> metrics = new ConcurrentHashMap<>();
	//
	private Histogram getMetric(String key) {
		Histogram metric = metrics.get(key);
		if ( metric == null ) {
			metric = new Histogram();
			metrics.put(key, metric);
		}
		return metric;
	}
	
	public void update(String key, long value) {
		Histogram metric = getMetric(key);
		metric.update(value);
	}

	public void inc(String key, int value) {
		Histogram metric = getMetric(key);
		metric.update(value);
	}

	
	public void reset() {
		for(Histogram m: metrics.values()) {
			m.reset();
		}
	}
	
	//
	public Map<String, Histogram> unmodifiableMap() {
		return Collections.unmodifiableMap( metrics );
	}

}
