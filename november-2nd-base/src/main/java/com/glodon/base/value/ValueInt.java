package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.MathUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueInt extends Value {

    private static final int STATIC_SIZE = 128;
    private static final int DYNAMIC_SIZE = 256;
    private static final ValueInt[] STATIC_CACHE = new ValueInt[STATIC_SIZE];
    private static final ValueInt[] DYNAMIC_CACHE = new ValueInt[DYNAMIC_SIZE];

    private final int value;

    static {
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueInt(i);
        }
    }

    private ValueInt(int value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueInt && value == ((ValueInt) other).value;
    }

    public static ValueInt get(int i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[i];
        }
        ValueInt v = DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)];
        if (v == null || v.value != i) {
            v = new ValueInt(i);
            DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)] = v;
        }
        return v;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return INT;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Integer a = (Integer) aObj;
            Integer b = (Integer) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 24;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            int x = (Integer) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            int x = v.getInt();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, int x) {
            if (x < 0) {
                if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_INT_MAX) {
                    buff.put((byte) TAG_INTEGER_FIXED).putInt(x);
                } else {
                    buff.put((byte) TAG_INTEGER_NEGATIVE).putVarInt(-x);
                }
            } else if (x <= 15) {
                buff.put((byte) (TAG_INTEGER_0_15 + x));
            } else if (x <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                buff.put((byte) INT).putVarInt(x);
            } else {
                buff.put((byte) TAG_INTEGER_FIXED).putInt(x);
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
                case INT:
                    return ValueInt.get(DataUtils.readVarInt(buff));
                case TAG_INTEGER_NEGATIVE:
                    return ValueInt.get(-DataUtils.readVarInt(buff));
                case TAG_INTEGER_FIXED:
                    return ValueInt.get(buff.getInt());
            }
            return ValueInt.get(tag - TAG_INTEGER_0_15);
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueInt v = (ValueInt) o;
        return MathUtils.compareInt(value, v.value);
    }
}
