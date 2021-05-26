package com.feeyo.raft.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Named thread factory with prefix.
 */
public class NamedThreadFactory implements ThreadFactory {

	private static final LogUncaughtExceptionHandler uncaughtExceptionHandler = new LogUncaughtExceptionHandler();

	private static final class LogUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			LOG.error("Uncaught exception in thread {}", t, e);
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(NamedThreadFactory.class);

	private final String prefix;

	private final AtomicInteger counter = new AtomicInteger(0);
	private final boolean daemon;

	public NamedThreadFactory(String prefix) {
		this(prefix, false);
	}

	public NamedThreadFactory(String prefix, boolean daemon) {
		super();
		this.prefix = prefix;
		this.daemon = daemon;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setDaemon(this.daemon);
		t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
		t.setName(this.prefix + counter.getAndIncrement());
		return t;
	}
}