package com.datapack.store;

import java.nio.file.Path;

public class LocalStore implements Store {

    private final Path basePath;

    public LocalStore(Path basePath) {
        this.basePath = basePath;
    }

    public Path getBasePath() {
        return basePath;
    }

    @Override
    public Path resolve(Location location) {
        return this.basePath.resolve(location.getPath());
    }
}
