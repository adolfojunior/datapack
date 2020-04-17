package com.datapack.bucket;

import com.datapack.data.DataValue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Bucket<T extends DataValue<?>> implements Iterable<T> {

	private final int limit;
	private final BucketKey key;
	private final AtomicInteger count;
	private final Collection<T> values;
	private final long creatinTime;

	public Bucket(BucketKey key, int limit) {
		this.key = key;
		this.limit = limit;
		this.count = new AtomicInteger();
		this.values = new ConcurrentLinkedDeque<>();
		this.creatinTime = System.currentTimeMillis();
	}

	public BucketKey getKey() {
		return key;
	}

	public int getLimit() {
		return limit;
	}

	public Collection<T> getValues() {
		return Collections.unmodifiableCollection(values);
	}

	public Stream<T> stream() {
		return getValues().stream();
	}

	public int size() {
		return this.count.get();
	}

	public boolean isFull() {
		return this.count.get() >= limit;
	}

	void add(T value) {
		this.values.add(value);
		this.count.incrementAndGet();
	}

	public LocalDateTime getTimestamp() {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(key.getTimestamp()), ZoneOffset.UTC);
	}

	public long getCreationTime() {
		return creatinTime;
	}

	public boolean isBefore(long timestamp) {
		return creatinTime < timestamp;
	}

	@Override
	public Iterator<T> iterator() {
		return values.iterator();
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public String toString() {
		return "{key=" + key + ", count=" + count + ", limit=" + limit + ", values=" + values + "}";
	}
}
