package com.glodon.storage.engine.btree;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.glodon.base.conf.Config;
import com.glodon.base.async.AsyncHandler;
import com.glodon.base.async.AsyncResult;
import com.glodon.base.storage.*;
import com.glodon.base.storage.type.ObjectDataType;
import com.glodon.base.util.DataUtils;
import com.glodon.storage.engine.btree.PageOperations.Append;
import com.glodon.storage.engine.btree.PageOperations.Put;
import com.glodon.storage.engine.btree.PageOperations.PutIfAbsent;
import com.glodon.storage.engine.btree.PageOperations.Remove;
import com.glodon.storage.engine.btree.PageOperations.Replace;
import com.glodon.storage.engine.btree.PageOperations.SingleWrite;
import com.glodon.base.storage.page.PageOperation;
import com.glodon.base.storage.page.PageOperation.PageOperationResult;
import com.glodon.base.storage.page.PageOperationHandler;
import com.glodon.base.storage.page.PageOperationHandlerFactory;
import com.glodon.base.storage.type.StorageDataType;
import com.glodon.base.value.ValueLong;

/**
 * BTree KV存储.
 * <p>
 * Created by liujing on 2023/10/16.
 */
public final class BTreeMap<K, V> implements StorageMap<K, V> {

    private final String name;
    private final Storage storage;
    private final boolean readOnly;
    private final boolean inMemory;
    private final StorageDataType keyType;
    private final StorageDataType valueType;
    private final Config config;
    private final BTreeStore btreeStore;
    private final PageOperationHandlerFactory pohFactory;
    private final AtomicLong maxKey = new AtomicLong(0);
    private final AtomicLong size = new AtomicLong(0);
    private PageStorageMode pageStorageMode = PageStorageMode.ROW_STORAGE;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private class RootPageReference extends PageReference {
        @Override
        public void replacePage(Page page) {
            super.replacePage(page);
            setRootRef(page);
        }
    }

    private final RootPageReference rootRef = new RootPageReference();
    private Page root;

    public BTreeMap(String name, StorageDataType keyType, StorageDataType valueType, Config config, Storage storage) {
        DataUtils.checkNotNull(name, "name");
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.storage = storage;

        DataUtils.checkNotNull(config, "config");
        this.config = config;
        this.readOnly = config.isReadOnly();
        this.inMemory = config.isInMemory();

        PageOperationHandlerFactory pageOperationHandlerFactory = config.getPageOperationHandlerFactory();
        this.pohFactory = pageOperationHandlerFactory == null ? PageOperationHandlerFactory.create(config) : pageOperationHandlerFactory;
        String mode = config.getPageStorageMode();
        if (mode != null) {
            pageStorageMode = PageStorageMode.valueOf(mode.toUpperCase());
        }
        //BTree存储器
        btreeStore = new BTreeStore(this);
        Chunk lastChunk = btreeStore.getLastChunk();
        if (lastChunk != null) {
            size.set(lastChunk.mapSize);
            Page root = btreeStore.readPage(lastChunk.rootPagePos);
            setRootRef(root);
            setMaxKey(lastKey());
        } else {
            root = BTreeLeaf.createEmpty(this);
            setRootRef(root);
        }
    }

    private void setRootRef(Page root) {
        if (this.root != root) {
            this.root = root;
        }
        if (rootRef.getPage() != root) {
            if (root.getRef() != rootRef) {
                root.setRef(rootRef);
                rootRef.replacePage(root);
            }
            if (root.isNode()) {
                for (PageReference ref : root.getChildren()) {
                    Page p = ref.getPage();
                    if (p != null && p.getParentRef() != rootRef)
                        p.setParentRef(rootRef);
                }
            }
        }
    }

    public Page getRootPage() {
        return root;
    }

    public void newRoot(Page newRoot) {
        setRootRef(newRoot);
    }

    private void acquireSharedLock() {
        lock.readLock().lock();
    }

    private void releaseSharedLock() {
        lock.readLock().unlock();
    }

    private void acquireExclusiveLock() {
        lock.writeLock().lock();
    }

    private void releaseExclusiveLock() {
        lock.writeLock().unlock();
    }

    public PageOperationHandlerFactory getPohFactory() {
        return pohFactory;
    }

    public Config getConfig() {
        return config;
    }

    public BTreeStore getBTreeStorage() {
        return btreeStore;
    }

    public PageStorageMode getPageStorageMode() {
        return pageStorageMode;
    }

    public void setPageStorageMode(PageStorageMode pageStorageMode) {
        this.pageStorageMode = pageStorageMode;
    }

    @Override
    public V get(K key) {
        return binarySearch(key, true);
    }

    public V get(K key, boolean allColumns) {
        return binarySearch(key, allColumns);
    }

    public V get(K key, int columnIndex) {
        return binarySearch(key, new int[]{columnIndex});
    }

    @Override
    public V get(K key, int[] columnIndexes) {
        return binarySearch(key, columnIndexes);
    }

    private V binarySearch(Object key, boolean allColumns) {
        Page p = root.gotoLeafPage(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, allColumns) : null;
    }

    private V binarySearch(Object key, int[] columnIndexes) {
        Page p = root.gotoLeafPage(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, columnIndexes) : null;
    }

    @Override
    public K firstKey() {
        return getFirstLast(true);
    }

    @Override
    public K lastKey() {
        return getFirstLast(false);
    }

    private K getFirstLast(boolean first) {
        if (isEmpty()) {
            return null;
        }
        Page p = root;
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    @Override
    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    @Override
    public K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    @Override
    public K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    @Override
    public K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    private K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        int x = p.getPageIndex(key);
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    @Override
    public long size() {
        return size.get();
    }

    public void incrementSize() {
        size.incrementAndGet();
    }

    @Override
    public void decrementSize() {
        size.decrementAndGet();
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean isInMemory() {
        return inMemory;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return cursor(CursorParameters.create(from));
    }

    @Override
    public StorageMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return new BTreeCursor<>(this, parameters);
    }

    @Override
    public void clear() {
        checkWrite();
        try {
            acquireExclusiveLock();
            root.removeAllRecursive();
            size.set(0);
            maxKey.set(0);
            newRoot(BTreeLeaf.createEmpty(this));
        } finally {
            releaseExclusiveLock();
        }
    }

    @Override
    public void remove() {
        try {
            acquireExclusiveLock();

            btreeStore.remove();
            closeMap();
        } finally {
            releaseExclusiveLock();
        }
    }

    @Override
    public boolean isClosed() {
        return btreeStore.isClosed();
    }

    @Override
    public void close() {
        try {
            acquireExclusiveLock();
            closeMap();
            btreeStore.close();
        } finally {
            releaseExclusiveLock();
        }
    }

    private void closeMap() {
        storage.closeMap(name);
    }

    @Override
    public void save() {
        try {
            acquireSharedLock();
            btreeStore.save();
        } finally {
            releaseSharedLock();
        }
    }

    public int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        return name;
    }

    public void printPage() {
        printPage(true);
    }

    public void printPage(boolean readOffLinePage) {
        System.out.println(root.getPrettyPageInfo(readOffLinePage));
    }

    public Page gotoLeafPage(Object key) {
        return root.gotoLeafPage(key);
    }

    public Page gotoLeafPage(Object key, boolean markDirty) {
        return root.gotoLeafPage(key, markDirty);
    }

    private void checkWrite(V value) {
        DataUtils.checkNotNull(value, "value");
        checkWrite();
    }

    private void checkWrite() {
        if (btreeStore.isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException("This map is read-only");
        }
    }

    @Override
    public void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        handler.handle(new AsyncResult<>(v));
    }

    @Override
    public V put(K key, V value) {
        return put0(key, value, null);
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(key, value, handler);
    }

    private V put0(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        Put<K, V, V> put = new Put<>(this, key, value, handler);
        return runPageOperation(put);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent0(key, value, null);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(key, value, handler);
    }

    private V putIfAbsent0(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        PutIfAbsent<K, V> putIfAbsent = new PutIfAbsent<>(this, key, value, handler);
        return runPageOperation(putIfAbsent);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace0(key, oldValue, newValue, null);
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(key, oldValue, newValue, handler);
    }

    private boolean replace0(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        checkWrite(newValue);
        Replace<K, V> replace = new Replace<>(this, key, oldValue, newValue, handler);
        return runPageOperation(replace);
    }

    @Override
    public K append(V value) {
        return append0(value, null);
    }

    @Override
    public K append(V value, AsyncHandler<AsyncResult<K>> handler) {
        return append0(value, handler);
    }

    private K append0(V value, AsyncHandler<AsyncResult<K>> handler) {
        checkWrite(value);
        Append<K, V> append = new Append<>(this, value, handler);
        return runPageOperation(append);
    }

    @Override
    public V remove(K key) {
        return remove0(key, null);
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(key, handler);
    }

    private V remove0(K key, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite();
        Remove<K, V> remove = new Remove<>(this, key, handler);
        return runPageOperation(remove);
    }

    private <R> R runPageOperation(SingleWrite<?, ?, R> po) {
        PageOperationHandler poHandler = getPageOperationHandler(false);
        if (po.run(poHandler) == PageOperationResult.SUCCEEDED) {
            return po.getResult();
        }
        poHandler = getPageOperationHandler(true);
        if (po.getResultHandler() == null) {
            PageOperation.Listener<R> listener = getPageOperationListener();
            po.setResultHandler(listener);
            poHandler.handlePageOperation(po);
            return listener.await();
        } else {
            poHandler.handlePageOperation(po);
            return null;
        }
    }

    private PageOperationHandler getPageOperationHandler(boolean useThreadPool) {
        Object t = Thread.currentThread();
        if (t instanceof PageOperationHandler) {
            return (PageOperationHandler) t;
        } else {
            if (useThreadPool) {
                return pohFactory.getPageOperationHandler();
            } else {
                return new PageOperationHandler.DummyPageOperationHandler();
            }
        }
    }

    private <R> PageOperation.Listener<R> getPageOperationListener() {
        Object object = Thread.currentThread();
        PageOperation.Listener<R> listener;
        if (object instanceof PageOperation.Listener)
            listener = (PageOperation.Listener<R>) object;
        else if (object instanceof PageOperation.ListenerFactory)
            listener = ((PageOperation.ListenerFactory<R>) object).createListener();
        else
            listener = new PageOperation.SyncListener<R>();
        listener.startListen();
        return listener;
    }


    public long incrementAndGetMaxKey() {
        return maxKey.incrementAndGet();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StorageDataType getKeyType() {
        return keyType;
    }

    @Override
    public StorageDataType getValueType() {
        return valueType;
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    @Override
    public void setMaxKey(K key) {
        if (key instanceof ValueLong) {
            long k = ((ValueLong) key).getLong();
            while (true) {
                long old = maxKey.get();
                if (k > old) {
                    if (maxKey.compareAndSet(old, k))
                        break;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public long getAndAddKey(long delta) {
        return maxKey.getAndAdd(delta);
    }

    public long getMaxKey() {
        return maxKey.get();
    }
}
