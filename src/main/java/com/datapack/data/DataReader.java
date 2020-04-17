package com.datapack.data;

import com.datapack.data.DataProto.Index;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public class DataReader<M extends Message> implements Closeable {

	private final InputFile<M> input;

	public DataReader(Path file, Parser<M> parser) throws IOException {
		this.input = new InputFile<>(file, parser);
	}

	public M read(Index entry) throws IOException {
		return input.read(entry.getOffset(), entry.getLength());
	}

	@Override
	public void close() throws IOException {
		this.input.close();
	}
}