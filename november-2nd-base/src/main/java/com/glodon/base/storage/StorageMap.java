package com.glodon.base.storage;

import com.glodon.base.async.AsyncHandler;
import com.glodon.base.async.AsyncResult;
import com.glodon.base.storage.type.StorageDataType;

/**
 * Created by liujing on 2023/10/12.
 */
public interface StorageMap<K, V> {

    String getName();

    StorageDataType getKeyType();

    StorageDataType getValueType();

    Storage getStorage();

    V get(K key);

    default V get(K key, int[] columnIndexes) {
        return get(key);
    }

    V put(K key, V value);

    V putIfAbsent(K key, V value);

    V remove(K key);

    boolean replace(K key, V oldValue, V newValue);

    K append(V value);

    void setMaxKey(K key);

    long getAndAddKey(long delta);

    K firstKey();

    K lastKey();

    K lowerKey(K key);

    K floorKey(K key);

    K higherKey(K key);

    K ceilingKey(K key);

    boolean areValuesEqual(Object a, Object b);

    long size();

    default void decrementSize() {
    }

    boolean containsKey(K key);

    boolean isEmpty();

    boolean isInMemory();

    StorageMapCursor<K, V> cursor(K from);

    default StorageMapCursor<K, V> cursor() {
        return cursor((K) null);
    }

    default StorageMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return cursor(parameters.from);
    }

    void clear();

    void remove();

    boolean isClosed();

    void close();

    void save();

    default void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        handleAsyncResult(handler, v);
    }

    default void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = put(key, value);
        handleAsyncResult(handler, v);
    }

    default void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = putIfAbsent(key, value);
        handleAsyncResult(handler, v);
    }

    default K append(V value, AsyncHandler<AsyncResult<K>> handler) {
        K k = append(value);
        handleAsyncResult(handler, k);
        return k;
    }

    default void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        Boolean b = replace(key, oldValue, newValue);
        handleAsyncResult(handler, b);
    }

    default void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = remove(key);
        handleAsyncResult(handler, v);
    }

    static <R> void handleAsyncResult(AsyncHandler<AsyncResult<R>> handler, R result) {
        AsyncResult<R> ar = new AsyncResult<>();
        ar.setResult(result);
        handler.handle(ar);
    }
}
