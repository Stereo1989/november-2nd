package com.glodon.base.storage.type;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.value.Value;
import com.glodon.base.util.DataUtils;

/**
 * Created by liujing on 2023/10/12.
 */
public interface StorageDataType {
    int TYPE_NULL = Value.NULL;
    int TYPE_BOOLEAN = Value.BOOLEAN;
    int TYPE_BYTE = Value.BYTE;
    int TYPE_SHORT = Value.SHORT;
    int TYPE_INT = Value.INT;
    int TYPE_LONG = Value.LONG;
    int TYPE_BIG_INTEGER = Value.TYPE_COUNT;
    int TYPE_FLOAT = Value.FLOAT;
    int TYPE_DOUBLE = Value.DOUBLE;
    int TYPE_BIG_DECIMAL = Value.DECIMAL;
    int TYPE_CHAR = Value.TYPE_COUNT + 1;
    int TYPE_STRING = Value.STRING;
    int TYPE_UUID = Value.UUID;
    int TYPE_DATE = Value.DATE;
    int TYPE_TIME = Value.TIME;
    int TYPE_TIMESTAMP = Value.TIMESTAMP;
    int TYPE_ARRAY = Value.TYPE_COUNT + 2;
    int TYPE_SERIALIZED_OBJECT = Value.TYPE_COUNT + 3;

    int TAG_BOOLEAN_TRUE = 32;
    int TAG_INTEGER_NEGATIVE = 33;
    int TAG_INTEGER_FIXED = 34;
    int TAG_LONG_NEGATIVE = 35;
    int TAG_LONG_FIXED = 36;
    int TAG_BIG_INTEGER_0 = 37;
    int TAG_BIG_INTEGER_1 = 38;
    int TAG_BIG_INTEGER_SMALL = 39;
    int TAG_FLOAT_0 = 40;
    int TAG_FLOAT_1 = 41;
    int TAG_FLOAT_FIXED = 42;
    int TAG_DOUBLE_0 = 43;
    int TAG_DOUBLE_1 = 44;
    int TAG_DOUBLE_FIXED = 45;
    int TAG_BIG_DECIMAL_0 = 46;
    int TAG_BIG_DECIMAL_1 = 47;
    int TAG_BIG_DECIMAL_SMALL = 48;
    int TAG_BIG_DECIMAL_SMALL_SCALED = 49;

    int TAG_INTEGER_0_15 = 64;
    int TAG_LONG_0_7 = 80;
    int TAG_STRING_0_15 = 88;
    int TAG_BYTE_ARRAY_0_15 = 104;

    int FLOAT_ZERO_BITS = Float.floatToIntBits(0.0f);
    int FLOAT_ONE_BITS = Float.floatToIntBits(1.0f);
    long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0.0d);
    long DOUBLE_ONE_BITS = Double.doubleToLongBits(1.0d);

    int compare(Object aObj, Object bObj);

    int getMemory(Object obj);

    void write(DataBuffer buff, Object obj);

    default void write(DataBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    Object read(ByteBuffer buff);

    default void read(ByteBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    default void writeMeta(DataBuffer buff, Object obj) {
    }

    default Object readMeta(ByteBuffer buff, int columnCount) {
        return null;
    }

    default void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        write(buff, obj);
    }

    default void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
    }

    default void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
    }

    default int getColumnCount() {
        return 1;
    }

    default int getMemory(Object obj, int columnIndex) {
        return getMemory(obj);
    }

    static int getTypeId(int tag) {
        int typeId;
        if (tag <= TYPE_SERIALIZED_OBJECT) {
            typeId = tag;
        } else {
            switch (tag) {
                case TAG_BOOLEAN_TRUE:
                    typeId = TYPE_BOOLEAN;
                    break;
                case TAG_INTEGER_NEGATIVE:
                case TAG_INTEGER_FIXED:
                    typeId = TYPE_INT;
                    break;
                case TAG_LONG_NEGATIVE:
                case TAG_LONG_FIXED:
                    typeId = TYPE_LONG;
                    break;
                case TAG_BIG_INTEGER_0:
                case TAG_BIG_INTEGER_1:
                case TAG_BIG_INTEGER_SMALL:
                    typeId = TYPE_BIG_INTEGER;
                    break;
                case TAG_FLOAT_0:
                case TAG_FLOAT_1:
                case TAG_FLOAT_FIXED:
                    typeId = TYPE_FLOAT;
                    break;
                case TAG_DOUBLE_0:
                case TAG_DOUBLE_1:
                case TAG_DOUBLE_FIXED:
                    typeId = TYPE_DOUBLE;
                    break;
                case TAG_BIG_DECIMAL_0:
                case TAG_BIG_DECIMAL_1:
                case TAG_BIG_DECIMAL_SMALL:
                case TAG_BIG_DECIMAL_SMALL_SCALED:
                    typeId = TYPE_BIG_DECIMAL;
                    break;
                default:
                    if (tag >= TAG_INTEGER_0_15 && tag <= TAG_INTEGER_0_15 + 15) {
                        typeId = TYPE_INT;
                    } else if (tag >= TAG_STRING_0_15 && tag <= TAG_STRING_0_15 + 15) {
                        typeId = TYPE_STRING;
                    } else if (tag >= TAG_LONG_0_7 && tag <= TAG_LONG_0_7 + 7) {
                        typeId = TYPE_LONG;
                    } else if (tag >= TAG_BYTE_ARRAY_0_15 && tag <= TAG_BYTE_ARRAY_0_15 + 15) {
                        typeId = TYPE_ARRAY;
                    } else {
                        throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                                "Unknown tag {0}", tag);
                    }
            }
        }
        return typeId;
    }
}
