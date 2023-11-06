package com.glodon.container.engine;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataType;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.Value;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueLong;

import java.nio.ByteBuffer;

/**
 * Created by liujing on 2023/10/24.
 */
public class ElementTagIndexValue extends Value implements ElementTagIndex {

    public static final int TEST_TAG_SECONDARY_INDEX_TYPE = Value.TYPE_COUNT + 5;

    private final ValueInt tagIndex;
    private final ValueLong idIndex;
    private int hash;

    private ElementTagIndexValue(ValueInt tagIndex, ValueLong idIndex) {
        this.tagIndex = tagIndex;
        this.idIndex = idIndex;
    }

    public final static ElementTagIndexValue get(ValueInt tagIndex) {
        return new ElementTagIndexValue(tagIndex, null);
    }

    public final static ElementTagIndexValue get(ValueInt tagIndex, ValueLong idIndex) {
        return new ElementTagIndexValue(tagIndex, idIndex);
    }

    @Override
    public int getType() {
        return TEST_TAG_SECONDARY_INDEX_TYPE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder("(");
        buff.append(tagIndex.getString());
        buff.append(idIndex != null ? idIndex.getString() : null);
        return buff.append(')').toString();
    }

    @Override
    public Object getObject() {
        Object[] vs = new Object[3];
        vs[0] = tagIndex.getObject();
        vs[2] = idIndex != null ? idIndex.getObject() : null;
        return vs;
    }

    public ValueInt getTagIndex() {
        return tagIndex;
    }

    public ValueLong getIdIndex() {
        return idIndex;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = 1;
        h = h * 31 + tagIndex.hashCode();
        if (idIndex != null) {
            h = h * 31 + idIndex.hashCode();
        }
        hash = h;
        return h;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ElementTagIndexValue)) {
            return false;
        }
        ElementTagIndexValue v = (ElementTagIndexValue) other;
        if (this == v) {
            return true;
        }
        if (tagIndex == null || !tagIndex.equals(v.tagIndex)) {
            return false;
        }

        if (idIndex == null || !idIndex.equals(v.idIndex)) {
            return false;
        }
        return true;
    }

    public boolean compareTag(ElementTagIndexValue o) {
        return this.tagIndex.equals(o.tagIndex);
    }

    @Override
    public int compareTo(Value o) {
        ElementTagIndexValue v = (ElementTagIndexValue) o;
        int compare = tagIndex.compareTo(v.getTagIndex());
        if (compare != 0) {
            return compare;
        }
//        int l = values.length;
//        int ol = v.values.length;
//        int len = Math.min(l, ol);
//        for (int i = 0; i < len; i++) {
//            Value v1 = values[i];
//            Value v2 = v.values[i];
//            int comp = v1.compareTo(v2);
//            if (comp != 0) {
//                return comp;
//            }
//        }
//        return l > ol ? 1 : l == ol ? 0 : -1;
        if (idIndex == null || v.idIndex == null) {
            return v.idIndex == null && idIndex == null ? 0 : (idIndex == null ? -1 : 1);
        } else {
            return idIndex.compareTo(v.idIndex);
        }
    }

    public static final StorageDataType type = new StorageDataType() {

        @Override
        public int compare(Object a, Object b) {
            if (a == b) {
                return 0;
            }
            return ((ElementTagIndexValue) a).compareTo((ElementTagIndexValue) b);
        }

        @Override
        public int getMemory(Object obj) {
            if (obj == null)
                return 0;
            ElementTagIndexValue v = (ElementTagIndexValue) obj;

            final ValueInt tagIndex = v.tagIndex;
            final ValueLong idIndex = v.idIndex;
            return v == null ?
                    0 : DataBuffer.TYPES[tagIndex.getType()].getMemory(tagIndex) + (idIndex == null ? 0 : DataBuffer.TYPES[idIndex.getType()].getMemory(idIndex));
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(DataBuffer buff, Object[] obj, int len) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            int type = buff.get();
            if (type == ElementTagIndexValue.TEST_TAG_SECONDARY_INDEX_TYPE) {
                ValueInt tagIndex = (ValueInt) DataBuffer.readValue(buff);
                ValueLong idIndex = (ValueLong) DataBuffer.readValue(buff);
                return ElementTagIndexValue.get(tagIndex, idIndex);
            } else {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}", type);
            }
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            ElementTagIndexValue v = (ElementTagIndexValue) obj;
            buff.put((byte) ElementTagIndexValue.TEST_TAG_SECONDARY_INDEX_TYPE);
            buff.writeValue(v.tagIndex);
            buff.writeValue(v.idIndex);
        }
    };

    @Override
    public ElementTagIndexValue getElementTagIndexValue() {
        return this;
    }
}
