package com.datapack.store;

import java.nio.file.Path;
import java.util.Objects;

public class Location {

    private final Path path;

    public Location(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    public String getFileName() {
        return path.getFileName().toString();
    }

    @Override
    public String toString() {
        return Objects.toString(path);
    }
}
