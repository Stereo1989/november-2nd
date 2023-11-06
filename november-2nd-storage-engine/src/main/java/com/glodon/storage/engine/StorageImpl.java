package com.glodon.storage.engine;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.glodon.base.conf.Config;
import com.glodon.base.storage.*;
import com.glodon.storage.engine.btree.BTreeMap;
import com.glodon.base.util.DataUtils;
import com.glodon.base.fs.FilePath;
import com.glodon.base.fs.FileUtils;
import com.glodon.base.storage.type.StorageDataType;

/**
 * 存储实现类
 * <p>
 * Created by liujing on 2023/10/13.
 */
public final class StorageImpl implements Storage {
    protected final Map<StorageEventListener, StorageEventListener> listeners = new ConcurrentHashMap<>();
    private final Map<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();
    private final Config config;
    private boolean closed;
    private boolean inMemory;

    private StorageImpl(Config config) {
        this.config = config;
        this.inMemory = config.isInMemory();
        String storagePath = getStoragePath();
        DataUtils.checkNotNull(storagePath, "storage path");
        if (!FileUtils.exists(storagePath)) {
            FileUtils.createDirectories(storagePath);
        }
        FilePath dir = FilePath.get(storagePath);
        for (FilePath fp : dir.newDirectoryStream()) {
            String mapFullName = fp.getName();
            if (mapFullName.startsWith(TEMP_NAME_PREFIX)) {
                fp.delete();
            }
        }
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType) {
        return openBTreeMap(name, keyType, valueType, null);
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType, Map<String, String> parameters) {
        return openBTreeMap(name, keyType, valueType, parameters);
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name) {
        return openBTreeMap(name, null, null, null);
    }

    private <K, V> BTreeMap<K, V> openBTreeMap(String name, StorageDataType keyType, StorageDataType valueType, Map<String, String> parameters) {
        StorageMap<?, ?> map = maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = maps.get(name);
                if (map == null) {
                    if (parameters != null) {
                        config.putAll(parameters);
                    }
                    map = new BTreeMap<>(name, keyType, valueType, config, this);
                    maps.put(name, map);
                }
            }
        }
        return (BTreeMap<K, V>) map;
    }

    @Override
    public void closeMap(String name) {
        StorageMap<?, ?> map = maps.remove(name);
        if (map != null) {
            map.close();
        }
    }

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public StorageMap<?, ?> getMap(String name) {
        return maps.get(name);
    }

    @Override
    public Set<String> getMapNames() {
        return new HashSet<>(maps.keySet());
    }

    @Override
    public String getStoragePath() {
        return config.getStoragePath();
    }

    @Override
    public boolean isInMemory() {
        return inMemory;
    }

    @Override
    public void save() {
        for (StorageMap<?, ?> map : maps.values()) {
            map.save();
        }
    }

    @Override
    public void drop() {
        close();
        if (!isInMemory()) {
            FileUtils.deleteRecursive(getStoragePath(), false);
        }
    }

    @Override
    public void close() {
        for (StorageEventListener listener : listeners.values()) {
            listener.beforeClose(this);
        }
        listeners.clear();
        save();
        closeNow();
    }

    @Override
    public void closeNow() {
        closed = true;
        for (StorageMap<?, ?> map : maps.values()) {
            map.close();
        }
        maps.clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void registerEventListener(StorageEventListener listener) {
        listeners.put(listener, listener);
    }

    @Override
    public void unregisterEventListener(StorageEventListener listener) {
        listeners.remove(listener);
    }

    public boolean isReadOnly() {
        return config.isReadOnly();
    }

    public final static class StorageBuilder {

        private final Config config = new Config();

        public StorageBuilder set(Config config) {
            this.config.putAll(config);
            return this;
        }

        public StorageBuilder set(String key, Object value) {
            config.setVal(key, value);
            return this;
        }

        public StorageBuilder storagePath(String storagePath) {
            return set(Config.STORAGE_PATH, storagePath);
        }

        public StorageBuilder readOnly() {
            return set(Config.STORAGE_READ_ONLY, true);
        }

        public StorageBuilder inMemory() {
            return set(Config.STORAGE_IN_MEMORY, true);
        }

        public StorageBuilder cacheSize(int mb) {
            return set(Config.STORAGE_CACHE_SIZE, mb * 1024 * 1024);
        }

        public StorageBuilder compress() {
            return set(Config.STORAGE_COMPRESS, 1);
        }

        public StorageBuilder compressHigh() {
            return set(Config.STORAGE_COMPRESS, 2);
        }

        public StorageBuilder pageSplitSize(int pageSplitSize) {
            return set(Config.STORAGE_PAGE_SPLIT_SIZE, pageSplitSize);
        }

        public StorageBuilder backgroundExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
            return set(Config.STORAGE_BACKGROUND_EXCEPTION_HANDLER, exceptionHandler);
        }

        public StorageBuilder minFillRate(int minFillRate) {
            return set(Config.STORAGE_MIN_FILL_RATE, minFillRate);
        }

        public Storage build() {
            return new StorageImpl(this.config);
        }
    }
}
