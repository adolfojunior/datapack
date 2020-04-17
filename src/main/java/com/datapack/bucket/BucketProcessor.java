package com.datapack.bucket;

import com.datapack.data.DataValue;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class BucketProcessor<V extends DataValue<?>> {

	public static class ProcessorOptions {

		private Duration granularity = Duration.ofMinutes(1);
		private int limit = 100;
		private int queueSize = 1;
		private int processors = 1;
		private Duration flushInterval = granularity = Duration.ofMinutes(1);

		public Duration getGranularity() {
			return granularity;
		}

		public void setGranularity(Duration granularity) {
			this.granularity = granularity;
		}

		public int getLimit() {
			return limit;
		}

		public void setLimit(int limit) {
			this.limit = limit;
		}

		public int getQueueSize() {
			return queueSize;
		}

		public void setQueueSize(int queueSize) {
			this.queueSize = queueSize;
		}

		public int getProcessors() {
			return processors;
		}

		public void setProcessors(int processors) {
			this.processors = processors;
		}

		public Duration getFlushInterval() {
			return flushInterval;
		}

		public void setFlushInterval(Duration flushInterval) {
			this.flushInterval = flushInterval;
		}

		long flushTimestamp() {
			return Instant.now().minus(flushInterval).toEpochMilli();
		}
	}

	private final ProcessorOptions options;
	private final BucketCollector<V> collector;
	private final BucketWindow<V> window;
	private final BlockingQueue<Bucket<V>> completed;

	private volatile ExecutorService processorExecutor;
	private volatile ScheduledExecutorService flushExecutor;

	public BucketProcessor(ProcessorOptions options, BucketCollector<V> collector) {
		this.window = new BucketWindow<>(options.granularity, options.limit, this::collect);
		this.completed = new ArrayBlockingQueue<>(options.queueSize);
		this.collector = collector;
		this.options = options;
	}

	public void start() {
		if (this.processorExecutor != null) {
			throw new IllegalStateException("processor already started");
		}

		// new flush interval
		this.processorExecutor = Executors.newFixedThreadPool(options.processors, threadFactory("processor"));
		this.flushExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory("processor-flush"));

		this.processorExecutor.submit((Runnable) this::process);
		this.flushExecutor.schedule((Runnable) this::flush, options.flushInterval.toMillis(), TimeUnit.MILLISECONDS);
	}

	private ThreadFactory threadFactory(String name) {
		return new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
	}

	public void shutdown(Duration timeout) {
		try {
			shutdown(this.processorExecutor, timeout);
			shutdown(this.flushExecutor, timeout);
		} finally {
			this.processorExecutor = null;
			this.flushExecutor = null;
		}
	}

	private void shutdown(ExecutorService executor, Duration timeout) {
		if (this.processorExecutor == null) {
			throw new IllegalStateException("processor not started");
		}
		try {
			executor.shutdown();
			executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			// TODO log
		}
	}

	public void add(V value) throws InterruptedException {
		window.add(value);
	}

	protected void process() {
		while (true) {
			try {
				collect(completed.take());
			} catch (InterruptedException e) {
				// TODO log
				break;
			}
		}
	}

	protected void flush() {
		window.releaseBefore(options.flushTimestamp());
	}

	protected void collect(Bucket<V> bucket) {
		collector.collect(bucket);
	}

	protected void complete(Bucket<V> bucket) {
		completed.add(bucket);
	}
}