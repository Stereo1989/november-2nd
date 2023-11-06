package com.glodon.storage.engine.btree;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;
import com.glodon.base.storage.type.StorageDataType;

/**
 * Btree 叶子结点实现
 * <p>
 * Created by liujing on 2023/10/16.
 */
public class BTreeLeaf extends LocalPage {

    private Object[] values;
    private volatile long totalCount;
    private ColumnPageReference[] columnPages;

    private static class ColumnPageReference {
        ColumnPage page;
        long pos;

        ColumnPageReference(long pos) {
            this.pos = pos;
        }
    }

    public BTreeLeaf(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return totalCount < 1;
    }

    @Override
    public Object getValue(int index) {
        return values[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        if (columnPages != null && columnIndexes != null) {
            for (int columnIndex : columnIndexes) {
                if (columnPages[columnIndex].page == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        if (columnPages != null && allColumns) {
            for (int columnIndex = 0, len = columnPages.length; columnIndex < len; columnIndex++) {
                if (columnPages[columnIndex].page == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = values[index];
        StorageDataType valueType = bTreeMap.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    @Override
    BTreeLeaf split(int at) {
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;

        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;

        totalCount = a;
        BTreeLeaf newPage = create(bTreeMap, bKeys, bValues, bKeys.length, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public long getTotalCount() {
        if (ASSERT) {
            long check = keys.length;
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                        "Expected: {0} got: {1}", check, totalCount);
            }
        }
        return totalCount;
    }

    @Override
    public Page copyLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        newKeys[index] = key;
        newValues[index] = value;
        bTreeMap.incrementSize();
        addMemory(bTreeMap.getKeyType().getMemory(key) + bTreeMap.getValueType().getMemory(value));
        BTreeLeaf newPage = create(bTreeMap, newKeys, newValues, totalCount + 1, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.setParentRef(getParentRef());
        newPage.setRef(getRef());
        removePage();
        return newPage;
    }

    @Override
    public void remove(int index) {
        int keyLength = keys.length;
        super.remove(index);
        Object old = values[index];
        addMemory(-bTreeMap.getValueType().getMemory(old));
        Object[] newValues = new Object[keyLength - 1];
        DataUtils.copyExcept(values, newValues, keyLength, index);
        values = newValues;
        totalCount--;
        bTreeMap.decrementSize();
    }

    @Override
    public void removeAllRecursive() {
        removePage();
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                     boolean disableCheck) {
        int mode = buff.get(buff.position() + 4);
        switch (PageStorageMode.values()[mode]) {
            case COLUMN_STORAGE:
                readColumnStorage(buff, chunkId, offset, expectedPageLength, disableCheck);
                break;
            default:
                readRowStorage(buff, chunkId, offset, expectedPageLength, disableCheck);
        }
    }

    private void readRowStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                                boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get();
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        buff = expandPage(buff, type, start, pageLength);

        bTreeMap.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        bTreeMap.getValueType().read(buff, values, keyLength);
        totalCount = keyLength;
        buff.getInt();
        recalculateMemory();
    }

    private void readColumnStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                                   boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get();
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        int columnCount = DataUtils.readVarInt(buff);
        columnPages = new ColumnPageReference[columnCount];
        keys = new Object[keyLength];
        int type = buff.get();
        for (int i = 0; i < columnCount; i++) {
            long pos = buff.getLong();
            columnPages[i] = new ColumnPageReference(pos);
        }
        buff = expandPage(buff, type, start, pageLength);

        bTreeMap.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        StorageDataType valueType = bTreeMap.getValueType();
        for (int row = 0; row < keyLength; row++) {
            values[row] = valueType.readMeta(buff, columnCount);
        }
        buff.getInt();
        totalCount = keyLength;
        recalculateMemory();
    }

    private void readColumnPage(int columnIndex) {
        ColumnPage page = (ColumnPage) bTreeMap.getBTreeStorage().readPage(columnPages[columnIndex].pos);
        if (page.values == null) {
            columnPages[columnIndex].page = page;
            page.readColumn(values, columnIndex);
            bTreeMap.getBTreeStorage().cachePage(columnPages[columnIndex].pos, page, page.getMemory());
        } else {
            values = page.values;
        }
    }

    @Override
    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        if (pos != 0) {
            return;
        }
        write(chunk, buff);
    }

    private void write(Chunk chunk, DataBuffer buff) {
        switch (bTreeMap.getPageStorageMode()) {
            case COLUMN_STORAGE:
                writeColumnStorage(chunk, buff);
                return;
            default:
                writeRowStorage(chunk, buff);
                return;
        }
    }

    private void writeRowStorage(Chunk chunk, DataBuffer buff) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0);
        buff.put((byte) bTreeMap.getPageStorageMode().ordinal());
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        int compressStart = buff.position();
        bTreeMap.getKeyType().write(buff, keys, keyLength);
        bTreeMap.getValueType().write(buff, values, keyLength);
        buff.putInt(0);

        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        updateChunkAndCachePage(chunk, start, pageLength, type);
        removeIfInMemory();
    }

    private void writeColumnStorage(Chunk chunk, DataBuffer buff) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0);
        buff.put((byte) bTreeMap.getPageStorageMode().ordinal());
        StorageDataType valueType = bTreeMap.getValueType();
        int columnCount = valueType.getColumnCount();
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength).putVarInt(columnCount);
        int typePos = buff.position();
        buff.put((byte) type);
        int columnPageStartPos = buff.position();
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(0);
        }
        int compressStart = buff.position();
        bTreeMap.getKeyType().write(buff, keys, keyLength);
        for (int row = 0; row < keyLength; row++) {
            valueType.writeMeta(buff, values[row]);
        }
        buff.putInt(0);
        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        long[] posArray = new long[columnCount];
        for (int col = 0; col < columnCount; col++) {
            ColumnPage page = new ColumnPage(bTreeMap, values, col);
            posArray[col] = page.write(chunk, buff);
        }
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

        updateChunkAndCachePage(chunk, start, pageLength, type);
        removeIfInMemory();
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        StorageDataType valueType = bTreeMap.getValueType();
        for (int i = 0; i < keys.length; i++) {
            mem += valueType.getMemory(values[i]);
        }
        addMemory(mem - memory);
    }

    @Override
    public BTreeLeaf copy() {
        return copy(true);
    }

    private BTreeLeaf copy(boolean removePage) {
        BTreeLeaf newPage = create(bTreeMap, keys, values, totalCount, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.setParentRef(getParentRef());
        newPage.setRef(getRef());
        if (removePage) {
            removePage();
        }
        return newPage;
    }

    static BTreeLeaf createEmpty(BTreeMap<?, ?> map) {
        return create(map, NULL, NULL, 0, PageUtils.PAGE_MEMORY);
    }

    static BTreeLeaf create(BTreeMap<?, ?> map, Object[] keys, Object[] values, long totalCount,
                            int memory) {
        BTreeLeaf p = new BTreeLeaf(map);
        p.keys = keys;
        p.values = values;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    @Override
    protected void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info) {
        buff.append(indent).append("values: ");
        for (int i = 0, len = keys.length; i < len; i++) {
            if (i > 0)
                buff.append(", ");
            buff.append(values[i]);
        }
        buff.append('\n');
    }
}
