package com.glodon.base.storage.type;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.value.Value;
import com.glodon.base.util.DataUtils;

/**
 * Created by liujing on 2023/10/12.
 */
public abstract class StorageDataTypeBase implements StorageDataType {

    public abstract int getType();

    public abstract void writeValue(DataBuffer buff, Value v);

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        return readValue(buff, tag).getObject();
    }

    public Object read(ByteBuffer buff, int tag) {
        return readValue(buff, tag).getObject();
    }

    public Value readValue(ByteBuffer buff) {
        throw newInternalError();
    }

    public Value readValue(ByteBuffer buff, int tag) {
        return readValue(buff);
    }

    protected IllegalStateException newInternalError() {
        return DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Internal error");
    }
}
