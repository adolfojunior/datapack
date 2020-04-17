package com.datapack.data;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OutputFile<V extends Message> implements Closeable {

    // default to 32kb
    private static final int AVERAGE_SIZE = 32 * 1024;

	private final OutputStream outputStream;
	private final CodedOutputStream codedOutput;

    public OutputFile(Path file) throws IOException {
    	this.outputStream = openOutput(file);
		this.codedOutput = CodedOutputStream.newInstance(outputStream, AVERAGE_SIZE);
    }

    protected OutputStream openOutput(Path file) throws IOException {
        // create parent folders
        Files.createDirectories(file.getParent());
        // then create the output
        return Files.newOutputStream(file, StandardOpenOption.CREATE);
    }

    public long size() {
        return this.codedOutput.getTotalBytesWritten();
    }

    public int write(V value) throws IOException {
        // clear the buffer
        long offset = size();
        // write the new object
		value.writeTo(this.codedOutput);
        // return object length
        return (int) (size() - offset);
    }

    public void flush() throws IOException {
        this.codedOutput.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
		this.outputStream.close();
    }
}