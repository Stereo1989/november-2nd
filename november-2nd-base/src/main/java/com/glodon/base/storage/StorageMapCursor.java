package com.glodon.base.storage;

import java.util.Iterator;

/**
 * Created by liujing on 2023/10/12.
 */
public interface StorageMapCursor<K, V> extends Iterator<K> {

    K getKey();

    V getValue();
}
