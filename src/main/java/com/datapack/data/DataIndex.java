package com.datapack.data;

import com.datapack.data.DataProto.Index;
import com.datapack.data.DataProto.IndexList;
import com.datapack.data.DataProto.Keys;

import java.util.HashMap;
import java.util.Map;

public class DataIndex {

    private final Map<Keys, Index> map = new HashMap<>();

    public DataIndex() {
    }

    public DataIndex(IndexList entries) {
        entries.getEntriesList().forEach(this::put);
    }

    public void putAll(DataIndex other) {
        map.putAll(other.map);
    }

    public void put(Index index) {
        map.put(index.getKeys(), index);
    }

    public IndexList getIndexList() {
        return IndexList.newBuilder().addAllEntries(map.values()).build();
    }
}
