package com.glodon.base.storage.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.*;

/**
 * Created by liujing on 2023/10/12.
 */
public class ObjectDataType implements StorageDataType {

    static final Class<?>[] COMMON_CLASSES = {boolean.class, byte.class, short.class, char.class,
            int.class, long.class, float.class, double.class, Object.class, Boolean.class, Byte.class,
            Short.class, Character.class, Integer.class, Long.class, BigInteger.class, Float.class,
            Double.class, BigDecimal.class, String.class, UUID.class, Date.class, Time.class,
            Timestamp.class};

    private static final HashMap<Class<?>, Integer> COMMON_CLASSES_MAP = new HashMap<>(COMMON_CLASSES.length);

    private StorageDataTypeBase last = ValueString.type;

    @Override
    public int compare(Object a, Object b) {
        switchType(a);
        return last.compare(a, b);
    }

    @Override
    public int getMemory(Object obj) {
        switchType(obj);
        return last.getMemory(obj);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        switchType(obj);
        last.write(buff, obj);
    }

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        int typeId = StorageDataType.getTypeId(tag);
        StorageDataTypeBase t = last;
        if (typeId != t.getType()) {
            last = t = newType(typeId);
        }
        return t.read(buff, tag);
    }

    private StorageDataTypeBase newType(int typeId) {
        switch (typeId) {
            case TYPE_NULL:
                return ValueNull.type;
            case TYPE_BOOLEAN:
                return ValueBoolean.type;
            case TYPE_BYTE:
                return ValueByte.type;
            case TYPE_SHORT:
                return ValueShort.type;
            case TYPE_CHAR:
                return new CharacterType();
            case TYPE_INT:
                return ValueInt.type;
            case TYPE_LONG:
                return ValueLong.type;
            case TYPE_FLOAT:
                return ValueFloat.type;
            case TYPE_DOUBLE:
                return ValueDouble.type;
            case TYPE_BIG_INTEGER:
                return new BigIntegerType();
            case TYPE_BIG_DECIMAL:
                return ValueDecimal.type;
            case TYPE_STRING:
                return ValueString.type;
            case TYPE_UUID:
                return ValueUuid.type;
            case TYPE_DATE:
                return ValueDate.type;
            case TYPE_TIME:
                return ValueTime.type;
            case TYPE_TIMESTAMP:
                return ValueTimestamp.type;
            case TYPE_ARRAY:
                return new ObjectArrayType();
            case TYPE_SERIALIZED_OBJECT:
                return new SerializedObjectType();
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}",
                typeId);
    }

    StorageDataTypeBase switchType(Object obj) {
        int typeId = getTypeId(obj);
        StorageDataTypeBase l = last;
        if (typeId != l.getType()) {
            last = l = newType(typeId);
        }
        return l;
    }

    private static int getTypeId(Object obj) {
        if (obj instanceof Integer) {
            return TYPE_INT;
        } else if (obj instanceof String) {
            return TYPE_STRING;
        } else if (obj instanceof Long) {
            return TYPE_LONG;
        } else if (obj instanceof Double) {
            return TYPE_DOUBLE;
        } else if (obj instanceof Float) {
            return TYPE_FLOAT;
        } else if (obj instanceof Boolean) {
            return TYPE_BOOLEAN;
        } else if (obj instanceof UUID) {
            return TYPE_UUID;
        } else if (obj instanceof Byte) {
            return TYPE_BYTE;
        } else if (obj instanceof Short) {
            return TYPE_SHORT;
        } else if (obj instanceof Character) {
            return TYPE_CHAR;
        } else if (obj == null) {
            return TYPE_NULL;
        } else if (isDate(obj)) {
            return TYPE_DATE;
        } else if (isTime(obj)) {
            return TYPE_TIME;
        } else if (isTimestamp(obj)) {
            return TYPE_TIMESTAMP;
        } else if (isBigInteger(obj)) {
            return TYPE_BIG_INTEGER;
        } else if (isBigDecimal(obj)) {
            return TYPE_BIG_DECIMAL;
        } else if (obj.getClass().isArray()) {
            return TYPE_ARRAY;
        }
        return TYPE_SERIALIZED_OBJECT;
    }

    private static boolean isBigInteger(Object obj) {
        return obj instanceof BigInteger && obj.getClass() == BigInteger.class;
    }

    private static boolean isBigDecimal(Object obj) {
        return obj instanceof BigDecimal && obj.getClass() == BigDecimal.class;
    }

    private static boolean isDate(Object obj) {
        return obj instanceof Date && obj.getClass() == Date.class;
    }

    private static boolean isTime(Object obj) {
        return obj instanceof Time && obj.getClass() == Time.class;
    }

    private static boolean isTimestamp(Object obj) {
        return obj instanceof Timestamp && obj.getClass() == Timestamp.class;
    }

    static Integer getCommonClassId(Class<?> clazz) {
        HashMap<Class<?>, Integer> map = COMMON_CLASSES_MAP;
        if (map.isEmpty()) {
            for (int i = 0, size = COMMON_CLASSES.length; i < size; i++) {
                COMMON_CLASSES_MAP.put(COMMON_CLASSES[i], i);
            }
        }
        return map.get(clazz);
    }

    static int compareNotNull(byte[] data1, byte[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            int b = data1[i] & 255;
            int b2 = data2[i] & 255;
            if (b != b2) {
                return b > b2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }
}
