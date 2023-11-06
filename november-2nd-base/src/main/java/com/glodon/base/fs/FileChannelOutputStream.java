package com.glodon.base.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by liujing on 2023/10/12.
 */
public class FileChannelOutputStream extends OutputStream {

    private final FileChannel channel;
    private final byte[] buffer = { 0 };

    public FileChannelOutputStream(FileChannel channel, boolean append) throws IOException {
        this.channel = channel;
        if (append) {
            channel.position(channel.size());
        } else {
            channel.position(0);
            channel.truncate(0);
        }
    }

    @Override
    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        FileUtils.writeFully(channel, ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(byte[] b) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
