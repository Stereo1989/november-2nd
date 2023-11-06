package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;

public class ValueFloat extends Value {

    public static final int ZERO_BITS = Float.floatToIntBits(0.0F);
    private static final ValueFloat ZERO = new ValueFloat(0.0F);
    private static final ValueFloat ONE = new ValueFloat(1.0F);

    private final float value;

    private ValueFloat(float value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.FLOAT;
    }

    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        long hash = Float.floatToIntBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    @Override
    public Object getObject() {
        return Float.valueOf(value);
    }

    public static ValueFloat get(float d) {
        if (d == 1.0F) {
            return ONE;
        } else if (d == 0.0F) {
            if (Float.floatToIntBits(d) == ZERO_BITS) {
                return ZERO;
            }
        }
        return new ValueFloat(d);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueFloat)) {
            return false;
        }
        return Float.compare(value, ((ValueFloat) other).value) == 0;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return FLOAT;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Float a = (Float) aObj;
            Float b = (Float) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 24;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            float x = (Float) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            float x = v.getFloat();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, float x) {
            int f = Float.floatToIntBits(x);
            if (f == FLOAT_ZERO_BITS) {
                buff.put((byte) TAG_FLOAT_0);
            } else if (f == FLOAT_ONE_BITS) {
                buff.put((byte) TAG_FLOAT_1);
            } else {
                int value = Integer.reverse(f);
                if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                    buff.put((byte) FLOAT).putVarInt(value);
                } else {
                    buff.put((byte) TAG_FLOAT_FIXED).putFloat(x);
                }
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
                case TAG_FLOAT_0:
                    return ValueFloat.get(0f);
                case TAG_FLOAT_1:
                    return ValueFloat.get(1f);
                case TAG_FLOAT_FIXED:
                    return ValueFloat.get(buff.getFloat());
            }
            return ValueFloat.get(Float.intBitsToFloat(Integer.reverse(DataUtils.readVarInt(buff))));
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueFloat v = (ValueFloat) o;
        return Float.compare(value, v.value);
    }
}
