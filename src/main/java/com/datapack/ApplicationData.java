package com.datapack;

import com.datapack.data.DataValue;

import static com.datapack.data.DataProto.Data;
import static com.datapack.data.DataProto.Keys;

public class ApplicationData implements DataValue<Data> {

    private final Data data;

    public ApplicationData(Data data) {
        this.data = data;
    }

    public static ApplicationData to(Data data) {
        return new ApplicationData(data);
    }

    @Override
    public long getTimestamp() {
        return data.getTimestamp();
    }

    @Override
    public Keys getKeys() { return data.getKeys(); }

    @Override
    public Data getValue() {
        return data;
    }
}