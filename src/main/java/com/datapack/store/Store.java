package com.datapack.store;

import java.nio.file.Path;

public interface Store {

    Path resolve(Location location);

    default Path load(Location location) {
        return resolve(location);
    }

    default Path update(Location location) {
        return resolve(location);
    }
}