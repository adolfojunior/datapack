package com.datapack.data;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class InputFile<V extends Message> implements Closeable {

	private final Parser<V> parser;
	private final FileChannel channel;

	public InputFile(Path file, Parser<V> parser) throws IOException {
		this.parser = parser;
		this.channel = FileChannel.open(file, StandardOpenOption.READ);
	}

	public V read() throws IOException {
		V value = parser.parseFrom(Channels.newInputStream(channel));
		if (value == null) {
			throw new IOException("unavailable to read data");
		}
		return value;
	}

	public V read(long offset, int length) throws IOException {
		ByteBuffer buffer = getByteBuffer(offset, length);
		V value = parser.parseFrom(CodedInputStream.newInstance(buffer));
		if (value == null) {
			throw new IOException("unavailable to read length " + length);
		}
		return value;
	}

	private ByteBuffer getByteBuffer(long offset, int length) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		channel.read(buffer, offset);
		return buffer;
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}
}