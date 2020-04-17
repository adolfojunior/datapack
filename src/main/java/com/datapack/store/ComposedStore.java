package com.datapack.store;

import java.nio.file.Path;

public class ComposedStore implements Store {

    private final Store store;

    protected ComposedStore(Store store) {
        this.store = store;
    }

    @Override
    public Path resolve(Location location) {
        return store.resolve(location);
    }
}