package com.glodon.base.storage.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.glodon.base.fs.FileBase;
import com.glodon.base.fs.FilePathWrapper;

/**
 * Created by liujing on 2023/10/12.
 */
public class FilePathCache extends FilePathWrapper {

    public static FileChannel wrap(FileChannel f) {
        return new FileCache(f);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileCache(getBase().open(mode));
    }

    @Override
    public String getScheme() {
        return "cache";
    }

    public static class FileCache extends FileBase {

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;
        private final FileChannel base;

        private final CacheLongKeyLIRS<ByteBuffer> cache;

        {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = 1024 * 1024;
            cache = new CacheLongKeyLIRS<ByteBuffer>(cc);
        }

        FileCache(FileChannel base) {
            this.base = base;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            base.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            base.position(newPosition);
            return this;
        }

        @Override
        public long position() throws IOException {
            return base.position();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return base.read(dst);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            long cachePos = getCachePos(position);
            int off = (int) (position - cachePos);
            int len = CACHE_BLOCK_SIZE - off;
            len = Math.min(len, dst.remaining());
            ByteBuffer buff = cache.get(cachePos);
            if (buff == null) {
                buff = ByteBuffer.allocate(CACHE_BLOCK_SIZE);
                long pos = cachePos;
                while (true) {
                    int read = base.read(buff, pos);
                    if (read <= 0) {
                        break;
                    }
                    if (buff.remaining() == 0) {
                        break;
                    }
                    pos += read;
                }
                int read = buff.position();
                if (read == CACHE_BLOCK_SIZE) {
                    cache.put(cachePos, buff, CACHE_BLOCK_SIZE);
                } else {
                    if (read <= 0) {
                        return -1;
                    }
                    len = Math.min(len, read - off);
                }
            }
            dst.put(buff.array(), off, len);
            return len == 0 ? -1 : len;
        }

        private static long getCachePos(long pos) {
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        @Override
        public long size() throws IOException {
            return base.size();
        }

        @Override
        public FileChannel truncate(long newSize) throws IOException {
            cache.clear();
            base.truncate(newSize);
            return this;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            clearCache(src, position);
            return base.write(src, position);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            clearCache(src, position());
            return base.write(src);
        }

        private void clearCache(ByteBuffer src, long position) {
            if (cache.size() > 0) {
                int len = src.remaining();
                long p = getCachePos(position);
                while (len > 0) {
                    cache.remove(p);
                    p += CACHE_BLOCK_SIZE;
                    len -= CACHE_BLOCK_SIZE;
                }
            }
        }

        @Override
        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return base.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return "cache:" + base.toString();
        }

    }

}
