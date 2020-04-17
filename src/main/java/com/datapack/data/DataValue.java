package com.datapack.data;

import com.datapack.data.DataProto.Keys;

public interface DataValue<T> {

	long getTimestamp();

	Keys getKeys();

	T getValue();
}
