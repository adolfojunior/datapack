package com.datapack.store;

import com.datapack.data.DataIndex;
import com.datapack.data.DataProto.Index;
import com.datapack.data.DataReader;
import com.datapack.data.DataValue;
import com.datapack.data.DataWriter;
import com.datapack.data.InputFile;
import com.datapack.data.OutputFile;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.datapack.data.DataProto.IndexList;

public class DataStore<V extends Message, D extends DataValue<V>> {

    public static final String INDEX = ".index";
    public static final String DATA = ".data";

    private final String storeId;
    private final Store store;
    private final Parser<V> parser;
    private final AtomicInteger fileCount;

    public DataStore(Store store, Parser<V> parser) {
        this.store = store;
        this.parser = parser;
        this.storeId = generateId();
        this.fileCount = new AtomicInteger();
    }

    private static String encode(int n) {
        return Integer.toString(Math.abs(n), 32);
    }

    protected String generateId() {
        String timeId = encode((int) System.currentTimeMillis());
        String randomId = encode(ThreadLocalRandom.current().nextInt());
        return timeId + "-" + randomId;
    }

    public DataIndex loadIndex(LocalDateTime timestamp) throws IOException {
        Path file = store.load(indexLocation(timestamp));
        try (InputFile<IndexList> input = new InputFile<>(file, IndexList.parser())) {
            return new DataIndex(input.read());
        }
    }

    public V loadData(Index index) throws IOException {
        LocalDateTime timestamp = dateTime(index.getTimestamp());
        Location location = toLocation(timestamp, index.getFile());
        Path file = store.load(location);
        try (DataReader<V> reader = new DataReader<>(file, parser)) {
            return reader.read(index);
        }
    }

    public void storeData(LocalDateTime timestamp, Stream<D> values) throws IOException {
        Location data = dataLocation(timestamp);
        Location index = indexLocation(timestamp);
        // store data and index
        writeIndex(index, writeData(data, values));
    }

    private DataIndex writeData(Location location, Stream<D> values) throws IOException {
        Path file = store.resolve(location);
        try (DataWriter<V, D> writer = new DataWriter<>(file)) {
            writer.writeAll(values::iterator);
            store.update(location);
            return writer.getIndex();
        }
    }

    private void writeIndex(Location location, DataIndex index) throws IOException {
        Path file = store.resolve(location);
        try (OutputFile<IndexList> output = new OutputFile<>(file)) {
            output.write(index.getIndexList());
            output.flush();
            store.update(location);
        }
    }

    protected Location dataLocation(LocalDateTime timestamp) {
        return toLocation(timestamp, fileName(timestamp, DATA));
    }

    protected Location indexLocation(LocalDateTime timestamp) {
        return toLocation(timestamp, fileName(timestamp, INDEX));
    }

    protected Location toLocation(LocalDateTime timestamp, String file) {
        Path path = folder(timestamp).resolve(file);
        return new Location(path);
    }

    protected Path folder(LocalDateTime timestamp) {
        String yearMonthDay = DateTimeFormatter.ISO_LOCAL_DATE.format(timestamp);
        String hour = pad(timestamp.getHour());
        return Paths.get(yearMonthDay, hour);
    }

    protected String fileName(LocalDateTime timestamp, String type) {
        String minuteAndSecond = pad(timestamp.getMinute()) + pad(timestamp.getSecond());
        return minuteAndSecond + "." + storeId + "." + fileId() + type;
    }

    private String pad(int n) {
        return n > 10 ? Integer.toString(n) : "0" + n;
    }

    private String fileId() {
        return encode(fileCount.incrementAndGet());
    }

    private LocalDateTime dateTime(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }
}
