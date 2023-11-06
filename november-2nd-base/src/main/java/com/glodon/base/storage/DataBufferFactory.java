package com.glodon.base.storage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liujing on 2023/10/12.
 */
public interface DataBufferFactory {

    DataBuffer create();

    DataBuffer create(int capacity);

    void recycle(DataBuffer buffer);

    static DataBufferFactory getConcurrentFactory() {
        return ConcurrentDataBufferFactory.INSTANCE;
    }

    /**
     * 可复用的数据缓存区
     */
    class ConcurrentDataBufferFactory implements DataBufferFactory {

        private static final ConcurrentDataBufferFactory INSTANCE = new ConcurrentDataBufferFactory();
        private static final int maxPoolSize = 20;
        private final AtomicInteger poolSize = new AtomicInteger();
        private final ConcurrentLinkedQueue<DataBuffer> queue = new ConcurrentLinkedQueue<>();

        @Override
        public DataBuffer create() {
            DataBuffer buffer = queue.poll();
            if (buffer == null)
                buffer = new DataBuffer();
            else {
                buffer.clear();
                poolSize.decrementAndGet();
            }
            return buffer;
        }

        @Override
        public DataBuffer create(int capacity) {
            DataBuffer buffer = null;
            for (int i = 0, size = poolSize.get(); i < size; i++) {
                buffer = queue.poll();
                if (buffer == null) {
                    break;
                }
                if (buffer.capacity() < capacity) {
                    queue.offer(buffer);
                } else {
                    buffer.clear();
                    poolSize.decrementAndGet();
                    return buffer;
                }
            }
            return DataBuffer.create(capacity);
        }

        @Override
        public void recycle(DataBuffer buffer) {
            if (buffer.capacity() <= DataBuffer.MAX_REUSE_CAPACITY) {
                if (poolSize.incrementAndGet() <= maxPoolSize) {
                    queue.offer(buffer);
                } else {
                    poolSize.decrementAndGet();
                }
            }
        }
    }
}
