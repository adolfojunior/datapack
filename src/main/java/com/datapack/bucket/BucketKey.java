package com.datapack.bucket;

import java.time.Instant;

public class BucketKey {

	private final long timestamp;

	public BucketKey(long timestamp, long granularity) {
		this.timestamp = timestamp - (timestamp % granularity);
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(timestamp);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		BucketKey other = (BucketKey) obj;
		return timestamp == other.timestamp;
	}

	@Override
	public String toString() {
		return Instant.ofEpochMilli(timestamp).toString();
	}
}
