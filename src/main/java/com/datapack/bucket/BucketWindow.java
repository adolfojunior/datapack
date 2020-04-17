package com.datapack.bucket;

import com.datapack.data.DataValue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BucketWindow<V extends DataValue<?>> {

	private final int limit;
	private final long granularity;

	private final BucketCollector<V> collector;
	private final Map<BucketKey, Bucket<V>> buckets;

	public BucketWindow(Duration granularity, int limit, BucketCollector<V> collector) {
		this.granularity = granularity.toMillis();
		this.limit = limit;
		this.collector = collector;
		this.buckets = createMap();
	}

	private ConcurrentHashMap<BucketKey, Bucket<V>> createMap() {
		return new ConcurrentHashMap<>();
	}

	public void add(V value) {

		BucketKey key = getKey(value);
		Bucket<V> bucket = getBucket(key);

		bucket.add(value);

		checkLimit(bucket);
	}

	protected BucketKey getKey(V value) {
		return new BucketKey(value.getTimestamp(), this.granularity);
	}

	protected Bucket<V> getBucket(BucketKey key) {
		return buckets.computeIfAbsent(key, this::createBucket);
	}

	private void checkLimit(Bucket<V> bucket) {
		if (bucket.isFull()) {
			tryRelease(bucket);
		}
	}

	protected void tryRelease(Bucket<V> bucket) {
		if (buckets.remove(bucket.getKey(), bucket)) {
			release(bucket);
		}
	}

	protected Bucket<V> createBucket(BucketKey key) {
		return new Bucket<V>(key, limit);
	}

	protected void release(Bucket<V> bucket) {
		collector.collect(bucket);
	}

	public void releaseBefore(long timestamp) {
		for (Bucket<V> bucket : buckets.values()) {
			if (bucket.isBefore(timestamp)) {
				tryRelease(bucket);
			}
		}
	}
}