package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.MathUtils;

public class ValueLong extends Value {

    private static final int STATIC_SIZE = 100;
    private static final ValueLong[] STATIC_CACHE;

    private final long value;

    static {
        STATIC_CACHE = new ValueLong[STATIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueLong(i);
        }
    }

    private ValueLong(long value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.LONG;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >> 32));
    }

    @Override
    public Object getObject() {
        return value;
    }

    public static ValueLong get(long i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[(int) i];
        }
        return new ValueLong(i);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLong && value == ((ValueLong) other).value;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return LONG;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Long a = (Long) aObj;
            Long b = (Long) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 30;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            long x = (Long) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            long x = v.getLong();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, long x) {
            if (x < 0) {
                if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) TAG_LONG_FIXED);
                    buff.putLong(x);
                } else {
                    buff.put((byte) TAG_LONG_NEGATIVE);
                    buff.putVarLong(-x);
                }
            } else if (x <= 7) {
                buff.put((byte) (TAG_LONG_0_7 + x));
            } else if (x <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                buff.put((byte) LONG);
                buff.putVarLong(x);
            } else {
                buff.put((byte) TAG_LONG_FIXED);
                buff.putLong(x);
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
                case LONG:
                    return ValueLong.get(DataUtils.readVarLong(buff));
                case TAG_LONG_NEGATIVE:
                    return ValueLong.get(-DataUtils.readVarLong(buff));
                case TAG_LONG_FIXED:
                    return ValueLong.get(buff.getLong());
            }
            return ValueLong.get(Long.valueOf(tag - TAG_LONG_0_7));
        }
    };

    @Override
    public int compareTo(Value o) {
        return MathUtils.compareLong(value, o.getLong());
    }
}
