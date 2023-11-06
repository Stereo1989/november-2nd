package com.glodon.container.engine;

import com.glodon.base.conf.Config;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.storage.Storage;
import com.glodon.base.storage.StorageMap;
import com.glodon.base.storage.StorageMapCursor;
import com.glodon.base.table.Scanner;
import com.glodon.base.table.Tablet;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.TinyLRUCache;
import com.glodon.base.value.*;
import com.glodon.storage.engine.StorageImpl;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 基于B+树实现数据表
 * <p>
 * clusteredIndex实际数据存储索引
 * <p>
 * Created by liujing on 2023/10/18.
 */
public class ElementTabletImpl implements Tablet<Long, ElementValue> {

    public final static String LRU_CAPACITY_KEY = "element.tablet.hash.lru.capacity";
    public final static String SYNC_SECONDARY_INDEX_RATE = "element.tablet.sync.secondary.index.rate";

    private final static long DEFAULT_SYNC_SECONDARY_INDEX_RATE = 15 * 1000;
    private final static int DEFAULT_LRU_CAPACITY = 1024 * 1024;
    private final static int THREAD_POOL_SIZE = 5;
    private final static String CLUSTERED_INDEX = "CLUSTERED_INDEX";
    private final static String TAG_SECONDARY_INDEX = "TAG_SECONDARY_INDEX";
    private final static String CATEGORY_SECONDARY_INDEX = "CATEGORY_SECONDARY_INDEX";

    private final String name;
    private final String baseDir;
    private Storage storage;
    private volatile boolean started = false;
    private StorageMap<Long, ElementValue> clusteredIndex;
    private StorageMap<ElementTagIndexValue, Value> tagSecondaryIndex;
    private StorageMap<ElementCategoryIndexValue, Value> categorySecondaryIndex;

    private ThreadPoolExecutor threadPool;
    private Thread syncSecondaryIndexThread;
    private TinyLRUCache<ValueInt, Set<ElementValue>> tagLRUCache;
    private TinyLRUCache<ValueUuid, Set<ElementValue>> categoryLRUCache;
    private final ReentrantLock lock = new ReentrantLock();

    public ElementTabletImpl(String baseDir, String name) {
        DataUtils.checkNotNull(name, "name");
        DataUtils.checkNotNull(baseDir, "baseDir");
        this.baseDir = baseDir;
        this.name = name;
    }

    @Override
    public void init(Config config) {
        if (!started) {
            lock.lock();
            try {
                this.storage = new StorageImpl.StorageBuilder()
                        .storagePath(new StringBuilder().append(baseDir).append(File.separator).append(name).toString())
                        .set(config)
                        .build();
                this.clusteredIndex = storage.openMap(CLUSTERED_INDEX, ValueLong.type, new ElementValueType());
                this.tagSecondaryIndex = storage.openMap(TAG_SECONDARY_INDEX, ElementTagIndexValue.type, ValueNull.type);
                this.categorySecondaryIndex = storage.openMap(CATEGORY_SECONDARY_INDEX, ElementCategoryIndexValue.type, ValueNull.type);
                this.threadPool = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
                this.threadPool.prestartAllCoreThreads();
                int capacity = config.getInt(LRU_CAPACITY_KEY, DEFAULT_LRU_CAPACITY);
                long syncSecondaryIndexRate = config.getLong(SYNC_SECONDARY_INDEX_RATE, DEFAULT_SYNC_SECONDARY_INDEX_RATE);
                this.tagLRUCache = TinyLRUCache.newInstance(capacity);
                this.categoryLRUCache = TinyLRUCache.newInstance(capacity);
                this.syncSecondaryIndexThread = new Thread(createSyncSecondaryIndexThread(syncSecondaryIndexRate));
                this.started = true;
                //启动同步二次索引线程
                this.syncSecondaryIndexThread.start();
                //预热LRU数据
                this.loadIndexFromDisk();
            } finally {
                lock.unlock();
            }
        } else {
            throw UnificationException.get(" tablet[%s] is started.", this.name);
        }
    }


    @Override
    public void close() {
        if (started) {
            lock.lock();
            try {
                this.storage.close();
                this.threadPool.shutdown();
                this.syncSecondaryIndexThread.interrupt();
                if (tagLRUCache != null) {
                    tagLRUCache.clear();
                }
                if (categoryLRUCache != null) {
                    categoryLRUCache.clear();
                }
                this.started = false;
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void truncate() {
        if (started) {
            lock.lock();
            try {
                if (!this.clusteredIndex.isClosed()) {
                    this.clusteredIndex.clear();
                }
                if (!this.tagSecondaryIndex.isClosed()) {
                    this.tagSecondaryIndex.clear();
                }
                if (!this.categorySecondaryIndex.isClosed()) {
                    this.categorySecondaryIndex.clear();
                }
                if (tagLRUCache != null) {
                    tagLRUCache.clear();
                }
                if (categoryLRUCache != null) {
                    categoryLRUCache.clear();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void drop() {
        if (started) {
            lock.lock();
            try {
                this.truncate();
                this.storage.drop();
                this.threadPool.shutdown();
                this.started = false;
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void checkpoint() throws InterruptedException {
        if (started) {
            this.syncSecondaryIndex();
            this.storage.save();
        }
    }

    @Override
    public void insert(final ElementValue[] datas) throws InterruptedException {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(datas, "datas");
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(3);
        try {
            //插入聚集索引
            threadPool.submit(() -> {
                for (ElementValue v : datas) {
                    long id = v.getId();
                    ElementTabletImpl.this.clusteredIndex.put(id, v);
                }
                latch.countDown();
            });
            //插入tag cache
            threadPool.submit(() -> {
                for (ElementValue v : datas) {
                    addElementTagIndexValue(v);
                }
                latch.countDown();
            });
            //插入category cache
            threadPool.submit(() -> {
                for (ElementValue v : datas) {
                    addElementCategoryIndexValue(v);
                }
                latch.countDown();
            });
            latch.await();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void insert(ElementValue data) {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(data, "data");
        long id = data.getId();
        lock.lock();
        try {
            this.clusteredIndex.put(id, data);
            this.addElementTagIndexValue(data);
            this.addElementCategoryIndexValue(data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean update(ElementValue data) {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(data, "data");
        lock.lock();
        try {
            Long id = data.getId();
            ElementValue old = this.clusteredIndex.get(id);
            this.removeElementCategoryIndexValue(old);
            this.addElementCategoryIndexValue(data);

            this.removeElementTagIndexValue(old);
            this.addElementTagIndexValue(data);
            return this.clusteredIndex.replace(data.getId(), old, data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean update(ElementValue[] datas) throws InterruptedException {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(datas, "datas");
        lock.lock();
        try {
            for (ElementValue value : datas) {
                boolean updated = update(value);
                if (!updated) {
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ElementValue select(Long id) {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(id, "id");
        return this.clusteredIndex.get(id);
    }

    @Override
    public void delete(Long id) {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(id, "id");
        lock.lock();
        try {
            ElementValue old = this.clusteredIndex.get(id);
            this.clusteredIndex.remove(id);
            this.removeElementTagIndexValue(old);
            this.removeElementCategoryIndexValue(old);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(final Long[] ids) throws InterruptedException {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(ids, "ids");
        if (ids.length == 0) {
            return;
        }
        lock.lock();
        try {
            final List<ElementValue> oldElementValues = new LinkedList<>();
            for (int i = 0; i < ids.length; i++) {
                ElementValue value = this.clusteredIndex.get(ids[i]);
                if (value != null) {
                    oldElementValues.add(value);
                }
            }
            CountDownLatch latch = new CountDownLatch(3);
            //删除聚集索引数据
            threadPool.submit(() -> {
                try {
                    for (Long id : ids) {
                        ElementTabletImpl.this.clusteredIndex.remove(id);
                    }
                } finally {
                    latch.countDown();
                }
            });
            //删除tag cache
            threadPool.submit(() -> {
                try {
                    for (ElementValue value : oldElementValues) {
                        this.removeElementTagIndexValue(value);
                    }
                } finally {
                    latch.countDown();
                }
            });
            //删除category cache
            threadPool.submit(() -> {
                try {
                    for (ElementValue value : oldElementValues) {
                        this.removeElementCategoryIndexValue(value);
                    }
                } finally {
                    latch.countDown();
                }
            });
            latch.await();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void scan(Scanner<Long, ElementValue> scanner) {
        if (!started) {
            throw UnificationException.get(" tablet[%s] is not started.", this.name);
        }
        DataUtils.checkNotNull(scanner, "scanner");
        StorageMapCursor<Long, ElementValue> cursor = this.clusteredIndex.cursor();
        while (cursor.hasNext()) {
            cursor.next();
            Long id = cursor.getKey();
            ElementValue elementValue = cursor.getValue();
            scanner.handle(id, elementValue);
        }
    }

    @Override
    public Set<ElementValue> in(Value v) {
        Set<ElementValue> elementValues = new HashSet<>();
        if (v instanceof ElementTagIndexValue) {
            ElementTagIndexValue start = (ElementTagIndexValue) v;
            Set<ElementValue> valueSet = tagLRUCache.get(start.getTagIndex());
            if (valueSet != null) {
                return Collections.unmodifiableSet(valueSet);
            } else {
                StorageMapCursor<ElementTagIndexValue, Value> cursor = this.tagSecondaryIndex.cursor(start);
                while (cursor.hasNext()) {
                    cursor.next();
                    ElementTagIndexValue tagIndexValue = cursor.getKey();
                    if (!start.compareTag(tagIndexValue)) {
                        break;
                    } else {
                        ElementValue elementValue = this.select(tagIndexValue.getIdIndex().getLong());
                        if (elementValue != null) {
                            elementValues.add(elementValue);
                        }
                    }
                }
            }
        } else if (v instanceof ElementCategoryIndexValue) {
            ElementCategoryIndexValue start = (ElementCategoryIndexValue) v;
            Set<ElementValue> valueSet = categoryLRUCache.get(start.getCategoryIndex());
            if (valueSet != null) {
                return Collections.unmodifiableSet(valueSet);
            } else {
                StorageMapCursor<ElementCategoryIndexValue, Value> cursor = this.categorySecondaryIndex.cursor(start);
                while (cursor.hasNext()) {
                    cursor.next();
                    ElementCategoryIndexValue categoryIndexValue = cursor.getKey();
                    if (!start.compareCategory(categoryIndexValue)) {
                        break;
                    } else {
                        ElementValue elementValue = this.select(categoryIndexValue.getIdIndex().getLong());
                        if (elementValue != null) {
                            elementValues.add(elementValue);
                        }
                    }
                }
            }
        }
        return elementValues;
    }

    private void loadIndexFromDisk() {
        this.scan(new Scanner<Long, ElementValue>() {
            @Override
            public void handle(Long aLong, ElementValue value) {
                addElementTagIndexValue(value);
                addElementCategoryIndexValue(value);
            }
        });
    }

    private void addElementCategoryIndexValue(ElementValue value) {
        if (value == null)
            return;
        ValueUuid category = value.getElementCategoryIndex().getCategoryIndex();
        Set<ElementValue> valueSet;
        if (null == (valueSet = categoryLRUCache.get(category))) {
            categoryLRUCache.put(category, valueSet = new HashSet<>());
        }
        valueSet.add(value);
    }

    private void removeElementCategoryIndexValue(ElementValue value) {
        if (value == null)
            return;
        ValueUuid category = value.getElementCategoryIndex().getCategoryIndex();
        Set<ElementValue> valueSet = categoryLRUCache.get(category);
        if (valueSet != null) {
            valueSet.remove(value);
        }
        if (valueSet.isEmpty()) {
            categoryLRUCache.remove(category);
        }
    }

    private void addElementTagIndexValue(ElementValue value) {
        if (value == null)
            return;
        ValueInt tag = value.getElementTagIndexValue().getTagIndex();
        Set<ElementValue> valueSet;
        if (null == (valueSet = tagLRUCache.get(tag))) {
            tagLRUCache.put(tag, valueSet = new HashSet<>());
        }
        valueSet.add(value);
    }

    private void removeElementTagIndexValue(ElementValue value) {
        if (value == null)
            return;
        ValueInt tag = value.getElementTagIndexValue().getTagIndex();
        Set<ElementValue> valueSet = tagLRUCache.get(tag);
        if (valueSet != null) {
            valueSet.remove(value);
        }
        if (valueSet.isEmpty()) {
            tagLRUCache.remove(tag);
        }
    }

    private synchronized void syncSecondaryIndex() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        this.tagSecondaryIndex.clear();
        this.categorySecondaryIndex.clear();
        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                ElementTabletImpl.this.scan(new Scanner<Long, ElementValue>() {
                    @Override
                    public void handle(Long aLong, ElementValue value) {
                        ElementTabletImpl.this.tagSecondaryIndex.put(value.getElementTagIndexValue(), ValueNull.INSTANCE);
                    }
                });
                latch.countDown();
            }
        });
        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                ElementTabletImpl.this.scan(new Scanner<Long, ElementValue>() {
                    @Override
                    public void handle(Long aLong, ElementValue value) {
                        ElementTabletImpl.this.categorySecondaryIndex.put(value.getElementCategoryIndex(), ValueNull.INSTANCE);
                    }
                });
                latch.countDown();
            }
        });
        latch.await();
    }

    Runnable createSyncSecondaryIndexThread(final long syncSecondaryIndexRate) {
        return new Runnable() {
            @Override
            public void run() {
                while (started && !Thread.currentThread().isInterrupted()) {
                    try {
                        syncSecondaryIndex();
                        Thread.sleep(syncSecondaryIndexRate);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        };
    }
}
