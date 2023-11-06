package com.glodon.container.engine;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataType;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.Value;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueLong;
import com.glodon.base.value.ValueUuid;

import java.nio.ByteBuffer;

/**
 * Created by liujing on 2023/10/24.
 */
public class ElementCategoryIndexValue extends Value implements ElementCategoryIndex {
    public static final int TEST_CATEGORY_SECONDARY_INDEX_TYPE = Value.TYPE_COUNT + 6;

    private final ValueUuid categoryIndex;
    private final ValueLong idIndex;
    private int hash;

    private ElementCategoryIndexValue(ValueUuid categoryIndex, ValueLong idIndex) {
        this.categoryIndex = categoryIndex;
        this.idIndex = idIndex;
    }

    public final static ElementCategoryIndexValue get(ValueUuid categoryIndex) {
        return new ElementCategoryIndexValue(categoryIndex, null);
    }

    public final static ElementCategoryIndexValue get(ValueUuid categoryIndex, ValueLong idIndex) {
        return new ElementCategoryIndexValue(categoryIndex, idIndex);
    }

    @Override
    public int getType() {
        return TEST_CATEGORY_SECONDARY_INDEX_TYPE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder("(");
        buff.append(categoryIndex.getString());
        buff.append(idIndex != null ? idIndex.getString() : null);
        return buff.append(')').toString();
    }

    @Override
    public Object getObject() {
        Object[] vs = new Object[3];
        vs[0] = categoryIndex.getObject();
        vs[2] = idIndex != null ? idIndex.getObject() : null;
        return vs;
    }

    public boolean compareCategory(ElementCategoryIndexValue o) {
        return this.categoryIndex.equals(o.categoryIndex);
    }

    public ValueUuid getCategoryIndex() {
        return categoryIndex;
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
        h = h * 31 + categoryIndex.hashCode();
        if (idIndex != null) {
            h = h * 31 + idIndex.hashCode();
        }
        hash = h;
        return h;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ElementCategoryIndexValue)) {
            return false;
        }
        ElementCategoryIndexValue v = (ElementCategoryIndexValue) other;
        if (this == v) {
            return true;
        }
        if (categoryIndex == null || !categoryIndex.equals(v.categoryIndex)) {
            return false;
        }

        if (idIndex == null || !idIndex.equals(v.idIndex)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Value o) {
        ElementCategoryIndexValue v = (ElementCategoryIndexValue) o;

        int compare = categoryIndex.compareTo(v.categoryIndex);
        if (compare != 0) {
            return compare;
        }

        if (idIndex == null || v.idIndex == null) {
            return v.idIndex == null && idIndex == null ? 0 : (idIndex == null ? -1 : 1);
        }
        return idIndex.compareTo(v.idIndex);
    }

    public static final StorageDataType type = new StorageDataType() {

        @Override
        public int compare(Object a, Object b) {
            if (a == b) {
                return 0;
            }
            return ((ElementCategoryIndexValue) a).compareTo((ElementCategoryIndexValue) b);
        }

        @Override
        public int getMemory(Object obj) {
            if (obj == null)
                return 0;
            ElementCategoryIndexValue v = (ElementCategoryIndexValue) obj;
            final ValueUuid categoryIndex = v.categoryIndex;
            final ValueLong idIndex = v.idIndex;
            return v == null ?
                    0 : DataBuffer.TYPES[categoryIndex.getType()].getMemory(categoryIndex) + (idIndex == null ? 0 : DataBuffer.TYPES[idIndex.getType()].getMemory(idIndex));
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
            if (type == ElementCategoryIndexValue.TEST_CATEGORY_SECONDARY_INDEX_TYPE) {
                ValueUuid categoryIndex = (ValueUuid) DataBuffer.readValue(buff);
                ValueLong idIndex = (ValueLong) DataBuffer.readValue(buff);
                return ElementCategoryIndexValue.get(categoryIndex, idIndex);
            } else {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}", type);
            }
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            ElementCategoryIndexValue v = (ElementCategoryIndexValue) obj;
            buff.put((byte) ElementCategoryIndexValue.TEST_CATEGORY_SECONDARY_INDEX_TYPE);
            buff.writeValue(v.categoryIndex);
            buff.writeValue(v.idIndex);
        }
    };

    @Override
    public ElementCategoryIndexValue getElementCategoryIndex() {
        return this;
    }
}
