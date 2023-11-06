package com.glodon.base.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class TinyLRUCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;
    private int size;

    private TinyLRUCache(int size) {
        super(size, (float) 0.75, true);
        this.size = size;
    }

    public static <K, V> TinyLRUCache<K, V> newInstance(int size) {
        return new TinyLRUCache<K, V>(size);
    }

    public void setMaxSize(int size) {
        this.size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > size;
    }

}
