package com.datapack.bucket;

import com.datapack.data.DataValue;

@FunctionalInterface
public interface BucketCollector<T extends DataValue<?>> {
	void collect(Bucket<T> bucket);
}