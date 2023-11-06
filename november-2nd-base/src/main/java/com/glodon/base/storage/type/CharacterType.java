package com.glodon.base.storage.type;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.value.Value;

import java.nio.ByteBuffer;


/**
 * Created by liujing on 2023/10/12.
 */
public class CharacterType extends StorageDataTypeBase {

    @Override
    public int getType() {
        return TYPE_CHAR;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        Character a = (Character) aObj;
        Character b = (Character) bObj;
        return a.compareTo(b);
    }

    @Override
    public int getMemory(Object obj) {
        return 24;
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        buff.put((byte) TYPE_CHAR);
        buff.putChar(((Character) obj).charValue());
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        return Character.valueOf(buff.getChar());
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

}
