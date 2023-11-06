package com.glodon.base.storage.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.Value;

/**
 * Created by liujing on 2023/10/12.
 */
public class SerializedObjectType extends StorageDataTypeBase {

    private int averageSize = 10000;
    private final ObjectDataType base = new ObjectDataType();

    @Override
    public int getType() {
        return TYPE_SERIALIZED_OBJECT;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        StorageDataType ta = getType(aObj);
        StorageDataType tb = getType(bObj);
        if (ta.getClass() != this.getClass() || tb.getClass() != this.getClass()) {
            if (ta == tb) {
                return ta.compare(aObj, bObj);
            }
        }
        if (aObj instanceof Comparable) {
            if (aObj.getClass().isAssignableFrom(bObj.getClass())) {
                return ((Comparable<Object>) aObj).compareTo(bObj);
            }
        }
        if (bObj instanceof Comparable) {
            if (bObj.getClass().isAssignableFrom(aObj.getClass())) {
                return -((Comparable<Object>) bObj).compareTo(aObj);
            }
        }
        byte[] a = serialize(aObj);
        byte[] b = serialize(bObj);
        return ObjectDataType.compareNotNull(a, b);
    }

    private StorageDataType getType(Object obj) {
        return base.switchType(obj);
    }

    @Override
    public int getMemory(Object obj) {
        StorageDataType t = getType(obj);
        if (t.getClass() == this.getClass()) {
            return averageSize;
        }
        return t.getMemory(obj);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        StorageDataType t = getType(obj);
        if (t.getClass() != this.getClass()) {
            t.write(buff, obj);
            return;
        }
        byte[] data = serialize(obj);
        int size = data.length * 2;
        averageSize = (size + 15 * averageSize) / 16;
        buff.put((byte) TYPE_SERIALIZED_OBJECT).putVarInt(data.length).put(data);
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        int len = DataUtils.readVarInt(buff);
        byte[] data = DataUtils.newBytes(len);
        buff.get(data);
        return deserialize(data);
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

    private static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not serialize {0}", obj, e);
        }
    }

    private static Object deserialize(byte[] data) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            return is.readObject();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not deserialize {0}",
                    Arrays.toString(data), e);
        }
    }
}
