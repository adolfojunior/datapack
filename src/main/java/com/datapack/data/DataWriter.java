package com.datapack.data;

import com.datapack.data.DataProto.Index;
import com.google.protobuf.Message;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public class DataWriter<M extends Message, D extends DataValue<M>> implements Closeable {

	private final Path file;
	private final DataIndex index;
	private final OutputFile<M> output;

	public DataWriter(Path file) throws IOException {
		this.file = file;
		this.index = new DataIndex();
		this.output = new OutputFile<>(file);
	}

	public DataIndex getIndex() {
		return index;
	}

	public void writeAll(Iterable<D> bucket) throws IOException {
		for (D value : bucket) {
			index.put(write(value));
		}
		this.output.flush();
	}

	protected Index write(D value) throws IOException {

		long offset = output.size();
		int length = output.write(value.getValue());

		return Index.newBuilder()
				.setKeys(value.getKeys())
				.setFile(file.getFileName().toString())
				.setOffset(offset)
				.setLength(length)
				.build();
	}

	@Override
	public void close() throws IOException {
		this.output.close();
	}
}