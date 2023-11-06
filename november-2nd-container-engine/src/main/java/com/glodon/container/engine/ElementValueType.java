package com.glodon.container.engine;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.Value;
import com.glodon.servingsphere.serialization.org.msgpack.MessagePack;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by liujing on 2023/10/16.
 */
public class ElementValueType extends StorageDataTypeBase {

    private final MessagePack messagePack = new MessagePack();

    @Override
    public int compare(Object aObj, Object bObj) {
        ElementValue e1 = (ElementValue) aObj;
        ElementValue e2 = (ElementValue) aObj;
        return e1.compareTo(e2);
    }

    @Override
    public int getMemory(Object obj) {
        ElementValue elementValue = (ElementValue) obj;
        return elementValue.getMemory();
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        byte[] data = serialize(obj);
        buff.put((byte) ElementValue.TEST_ELEMENT_TAG).putVarInt(data.length).put(data);
//        ElementValue elementValue = (ElementValue) obj;
//        //type
//        buff.put((byte) ElementValue.TEST_ELEMENT_TAG);
//        //id
//        buff.putLong(elementValue.getId());
//        //category
//        byte[] categoryBytes = elementValue.getCategoryBytes();
//        buff.putInt(categoryBytes.length);
//        buff.put(categoryBytes);
//        //tag
//        buff.putInt(elementValue.getTag());
//        //path
//        byte[] pathBytes = elementValue.getPathBytes();
//        if (pathBytes == null) {
//            buff.putInt(0);
//        } else {
//            buff.putInt(pathBytes.length);
//            buff.put(pathBytes);
//        }
//        byte[] contentBytes = elementValue.getContent();
//        if (contentBytes == null) {
//            buff.putInt(0);
//        } else {
//            buff.putInt(contentBytes.length);
//            buff.put(contentBytes);
//        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        int type = buff.get();
        if (type == getType()) {
            int len = DataUtils.readVarInt(buff);
            byte[] data = DataUtils.newBytes(len);
            buff.get(data);
            return deserialize(data);
        } else {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}", type);
        }

//        int type = buff.get();
//        if (type == getType()) {
//            long id = DataUtils.readVarLong(buff);
//
//            int categoryLen = DataUtils.readVarInt(buff);
//            byte[] categoryData = DataUtils.newBytes(categoryLen);
//            buff.get(categoryData);
//
//            int tag = DataUtils.readVarInt(buff);
//
//            int pathLen = DataUtils.readVarInt(buff);
//            byte[] pathData = null;
//            if (pathLen > 0) {
//                pathData = DataUtils.newBytes(pathLen);
//                buff.get(pathData);
//            }
//
//            int contentLen = DataUtils.readVarInt(buff);
//            byte[] contentData = null;
//            if (contentLen > 0) {
//                contentData = DataUtils.newBytes(contentLen);
//                buff.get(contentData);
//            }
//            ElementValue elementValue = new ElementValue();
//            elementValue.setId(id);
//            elementValue.setCategory(categoryData);
//            elementValue.setTag(tag);
//            elementValue.setPath(pathData);
//            elementValue.setContent(contentData);
//            return elementValue;
//        } else {
//            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}", type);
//        }
    }

    @Override
    public int getType() {
        return ElementValue.TEST_ELEMENT_TAG;
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

    private byte[] serialize(Object obj) {
        try {
            return messagePack.write(obj);
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not serialize {0}", obj, e);
        }
    }

    private Object deserialize(byte[] data) {
        try {
            return messagePack.read(data, ElementValue.class);
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not deserialize {0}",
                    Arrays.toString(data), e);
        }
    }
}
