package com.datapack;

import com.datapack.bucket.Bucket;
import com.datapack.bucket.BucketCollector;
import com.datapack.bucket.BucketProcessor;
import com.datapack.bucket.BucketProcessor.ProcessorOptions;
import com.datapack.data.DataProto.Data;
import com.datapack.store.DataStore;
import com.datapack.store.LocalStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datapack.data.DataProto.Keys;

public class Application {

    public static void main(String[] args) throws Exception {

        final LocalStore localStore = new LocalStore(Paths.get("localStore"));
        final S3Store s3Store = new S3Store(localStore);
        final DataStore<Data, ApplicationData> dataStore = new DataStore<>(localStore, Data.parser());
        final BucketCollector<ApplicationData> collector = new BucketCollector<ApplicationData>() {
            @Override
            public void collect(Bucket<ApplicationData> bucket) {
                try {
                    dataStore.storeData(bucket.getTimestamp(), bucket.stream());
                } catch (IOException e) {
                    // TODO log
                    e.printStackTrace();
                }
            }
        };

        final ProcessorOptions options = new ProcessorOptions();
        final BucketProcessor<ApplicationData> bucketProcessor = new BucketProcessor<>(options, collector);

        // emulate producers threads like Kafka
        produces(bucketProcessor, 5);

        // start the aggregation threads and flush
        bucketProcessor.start();

        // main thread wait
        TimeUnit.DAYS.sleep(Integer.MAX_VALUE);
    }

    private static void produces(BucketProcessor<ApplicationData> bucketProcessor, int numProducers) {

        final AtomicInteger count = new AtomicInteger();
        final ExecutorService executorService = Executors.newFixedThreadPool(numProducers);

        for (int i = 0; i < numProducers; i++) {
            executorService.submit(() -> {
                while (true) {
                    try {

                        Keys keys = Keys.newBuilder()
                                .putMap("si", Long.toHexString(System.currentTimeMillis()))
                                .build();

                        Data data = Data.newBuilder()
                                .setKeys(keys)
                                .setTimestamp(System.currentTimeMillis())
                                .setData("some data" + count.incrementAndGet())
                                .build();

                        bucketProcessor.add(ApplicationData.to(data));

                        TimeUnit.MILLISECONDS.sleep(300);

                    } catch (InterruptedException e) {
                        break;
                    }
                }
            });
        }
    }
}
