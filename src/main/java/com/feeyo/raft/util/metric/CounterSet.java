package com.feeyo.raft.util.metric;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.feeyo.metrics.Counter;

public class CounterSet {
	
	private Map<String, Counter> metrics = new ConcurrentHashMap<>();
	
	//
	private Counter getMetric(String key) {
		Counter metric = metrics.get(key);
		if ( metric == null ) {
			metric = new Counter();
			metrics.put(key, metric);
		}
		return metric;
	}
	
	public void inc(String key) {
		Counter metric = getMetric(key);
		metric.inc();
	}

	public void inc(String key, long n) {
		Counter metric = getMetric(key);
		metric.inc(n);
	}

	public void dec(String key) {
		Counter metric = getMetric(key);
		metric.dec();
	}

	public void dec(String key, long n) {
		Counter metric = getMetric(key);
		metric.dec(n);
	}
	
	public void reset() {
		for(Counter m: metrics.values()) {
			m.reset();
		}
	}
	
	//
	public Map<String, Counter> unmodifiableMap() {
		return Collections.unmodifiableMap( metrics );
	}
	
}