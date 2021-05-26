package com.feeyo.raft;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.metrics.Counter;
import com.feeyo.metrics.Gauge;
import com.feeyo.metrics.Histogram;
import com.feeyo.metrics.Metric;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.util.metric.CounterSet;
import com.feeyo.raft.util.metric.GaugeSet;
import com.feeyo.raft.util.metric.HistogramSet;


/**
 * Storage 、IO、Callback metrics.
 * 
 * @author zhuam
 *
 */
public final class RaftStatistics {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RaftStatistics.class );
	//
	private static final String lineSeparator = System.lineSeparator();
	//
	private static final String RAFT_PREFIX 	= "raft.";
	public static final String ON_READY 		= RAFT_PREFIX + "on-ready";
	public static final String APPEND_LOGS 		= RAFT_PREFIX + "append-logs";
	public static final String APPEND_STORAGE 	= RAFT_PREFIX + "append-storage";
	public static final String BCAST_MSG 		= RAFT_PREFIX + "bcast-msgs";
	public static final String APPLY_ENTRIES 	= RAFT_PREFIX + "apply-entries";
	public static final String APPLY_SNAPSHOT 	= RAFT_PREFIX + "apply-snapshot";
	public static final String CALLER_CB 		= RAFT_PREFIX + "caller.cb";
	//
	private CounterSet counterSet = new CounterSet();
	private HistogramSet histogramSet = new HistogramSet();
	private GaugeSet gaugeSet = new GaugeSet();
	//
	// the initial size roughly fits ~150 metrics with default scope settings
	private int previousSize = 16384;
	//
	public void print(String ident) {
		try {
			StringBuilder builder = new StringBuilder((int) (previousSize * 1.1));
			builder
			.append(lineSeparator)
			.append("=========================== ")
			.append( ident ).append(" metrics report ")
			.append("===========================")
			.append(lineSeparator);
			///
			for(Map.Entry<String, Counter> m : counterSet.unmodifiableMap().entrySet()) {
				if ( m.getValue().count() > 0 ) {
					builder
					.append( m.getKey() )
					.append( ", " )
					.append( String.format("count=%d", m.getValue().count()) )
					.append(lineSeparator);
				}
			}
			
			for(Map.Entry<String, Histogram> m : histogramSet.unmodifiableMap().entrySet()) {
				if ( m.getValue().count() > 0 ) {
					builder
					.append( m.getKey() )
					.append( ", " )
					.append( String.format("count=%d", m.getValue().count()) )
					.append( ", " )
//					.append( String.format("sum=%d", m.getValue().sum()) )
//					.append( ", " )
					.append( String.format("max=%d", m.getValue().max()) )
					.append( ", " )
					.append( String.format("min=%d", m.getValue().min()) )
					.append( ", " )
					.append( String.format("avg=%d", m.getValue().avg()) )
					.append(lineSeparator);
				}
			}
			//
			for(Map.Entry<String, Metric> m : gaugeSet.getMetrics().entrySet()) {
				@SuppressWarnings("unchecked")
				Gauge<Long> g = (Gauge<Long>)m.getValue();
				if ( g.getValue() > 0 ) {
					builder
					.append( m.getKey() )
					.append( "=" )
					.append( String.format("%d",  g.getValue()) )
					.append(lineSeparator);
				}
			}
			builder.append(lineSeparator);
			//
			LOGGER.info( builder.toString() );
			//
			previousSize = builder.length();
			
			// reset
			counterSet.reset();
			histogramSet.reset();
			
		} catch (Throwable e) {
			LOGGER.warn("metrics report err:", e);
		}
	}
	
	//
	public void gauge(long committed, long applied) {
		gaugeSet.update(committed, applied);
	}
	
	public void update(String key, long value) {
		histogramSet.update(key, value);
	}
	
	public void inc(Raftpb.Message msg) {
		counterSet.inc( msg.getMsgType().toString() );
	}

	public void inc(Raftpb.Message msg, long n) {
		counterSet.inc( msg.getMsgType().toString(), n );
	}
	///
	public Map<String, Histogram> getHistogramSet() {
		return histogramSet.unmodifiableMap();
	}

	public Map<String, Counter> getCounterSet() {
		return counterSet.unmodifiableMap();
	}

	public Map<String, Metric> getGaugeSet() {
		return gaugeSet.getMetrics();
	}
}
