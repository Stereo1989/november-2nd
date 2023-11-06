package com.glodon.storage.engine.btree;

import com.glodon.base.storage.CursorParameters;
import com.glodon.base.storage.StorageMapCursor;

/**
 * Created by liujing on 2023/10/16.
 */
public class BTreeCursor<K, V> implements StorageMapCursor<K, V> {

    protected final BTreeMap<K, ?> map;
    protected final CursorParameters<K> parameters;
    protected CursorPos pos;

    private K key;
    private V value;

    public BTreeCursor(BTreeMap<K, ?> map, CursorParameters<K> parameters) {
        this.map = map;
        this.parameters = parameters;
        min(map.getRootPage(), parameters.from);
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public boolean hasNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                return true;
            }
            pos = pos.parent;
            if (pos == null) {
                return false;
            }
            if (pos.index < map.getChildPageCount(pos.page)) {
                min(pos.page.getChildPage(pos.index++), null);
            }
        }
        return false;
    }

    @Override
    public K next() {
        int index = pos.index++;
        key = (K) pos.page.getKey(index);
        if (parameters.allColumns)
            value = (V) pos.page.getValue(index, true);
        else
            value = (V) pos.page.getValue(index, parameters.columnIndexes);
        return key;
    }

    protected void min(Page p, K from) {
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            int x = from == null ? 0 : p.getPageIndex(from);
            pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
    }
}
