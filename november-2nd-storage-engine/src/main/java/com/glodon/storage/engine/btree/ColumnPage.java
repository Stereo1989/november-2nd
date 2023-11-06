package com.glodon.storage.engine.btree;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataType;

class ColumnPage extends Page {

    Object[] values;
    private int columnIndex;
    private ByteBuffer buff;

    ColumnPage(BTreeMap<?, ?> map) {
        super(map);
    }

    ColumnPage(BTreeMap<?, ?> map, Object[] values, int columnIndex) {
        super(map);
        this.values = values;
        this.columnIndex = columnIndex;
    }

    @Override
    public int getMemory() {
        int memory = 0;
        if (values != null) {
            StorageDataType valueType = bTreeMap.getValueType();
            for (int row = 0, rowCount = values.length; row < rowCount; row++) {
                memory += valueType.getMemory(values[row], columnIndex);
            }
        }
        return memory;
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                     boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);
        buff.get();
        int compressType = buff.get();
        this.buff = expandPage(buff, compressType, start, pageLength);
    }

    void readColumn(Object[] values, int columnIndex) {
        this.values = values;
        this.columnIndex = columnIndex;
        StorageDataType valueType = bTreeMap.getValueType();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.readColumn(buff, values[row], columnIndex);
        }
        buff = null;
    }

    long write(Chunk chunk, DataBuffer buff) {
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_COLUMN;
        buff.putInt(0);

        StorageDataType valueType = bTreeMap.getValueType();
        int checkPos = buff.position();
        buff.putShort((short) 0);
        buff.put((byte) type);
        int compressTypePos = buff.position();
        int compressType = 0;
        buff.put((byte) compressType);
        int compressStart = buff.position();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.writeColumn(buff, values[row], columnIndex);
        }
        compressPage(buff, compressStart, compressType, compressTypePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);
        updateChunkAndCachePage(chunk, start, pageLength, type);
        return pos;
    }
}
