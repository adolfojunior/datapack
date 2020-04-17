package com.datapack;

import com.datapack.store.ComposedStore;
import com.datapack.store.DataStore;
import com.datapack.store.LocalStore;
import com.datapack.store.Location;

import java.nio.file.Path;

class S3Store extends ComposedStore {

    S3Store(LocalStore store) {
        super(store);
        // provide background data-rotation thread
        //            try (Stream<Path> files = Files.walk(store.getBasePath())) {
        //                files.sorted(Comparator.reverseOrder()).forEach(...);
        //            } catch (IOException e) {
        //                e.printStackTrace();
        //            }
    }

    protected void s3Load(Location location, Path path) {
        // load s3 location to path
        String fileName = location.getFileName();
        if (fileName.endsWith(DataStore.INDEX)) {
            // load all index available for the same minute and aggregate "--exclude '*' --include fileName.substring(fileName.indexOf('.')) + ".\\w+.\\w+\\.index"
            System.out.printf("[%s] will load all indexes close to %s %n", Thread.currentThread().getName(), location);
        } else {
            // load data
            System.out.printf("[%s] will load data from %s %n", Thread.currentThread().getName(), location);
        }
    }

    protected void s3Update(Path path, Location location) {
        // update path to s3 location
        System.out.printf("[%s] will check s3 for an update on %s %n", Thread.currentThread().getName(), location);
    }

    @Override
    public Path load(Location location) {
        Path path = super.update(location);
        s3Load(location, path);
        return path;
    }

    @Override
    public Path update(Location location) {
        Path path = super.update(location);
        s3Update(path, location);
        return path;
    }
}