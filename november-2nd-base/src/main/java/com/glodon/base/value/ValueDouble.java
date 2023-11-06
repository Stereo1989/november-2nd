package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueDouble extends Value {

    public static final long ZERO_BITS = Double.doubleToLongBits(0.0);
    private static final ValueDouble ZERO = new ValueDouble(0.0);
    private static final ValueDouble ONE = new ValueDouble(1.0);
    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.DOUBLE;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        long hash = Double.doubleToLongBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    @Override
    public Object getObject() {
        return Double.valueOf(value);
    }

    public static ValueDouble get(double d) {
        if (d == 1.0) {
            return ONE;
        } else if (d == 0.0) {
            if (Double.doubleToLongBits(d) == ZERO_BITS) {
                return ZERO;
            }
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return new ValueDouble(d);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueDouble)) {
            return false;
        }
        return Double.compare(value, ((ValueDouble) other).value) == 0;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return DOUBLE;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Double a = (Double) aObj;
            Double b = (Double) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 30;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            double x = (Double) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            double x = v.getDouble();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, double x) {
            long d = Double.doubleToLongBits(x);
            if (d == DOUBLE_ZERO_BITS) {
                buff.put((byte) TAG_DOUBLE_0);
            } else if (d == DOUBLE_ONE_BITS) {
                buff.put((byte) TAG_DOUBLE_1);
            } else {
                long value = Long.reverse(d);
                if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) DOUBLE);
                    buff.putVarLong(value);
                } else {
                    buff.put((byte) TAG_DOUBLE_FIXED);
                    buff.putDouble(x);
                }
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
                case TAG_DOUBLE_0:
                    return ValueDouble.get(0d);
                case TAG_DOUBLE_1:
                    return ValueDouble.get(1d);
                case TAG_DOUBLE_FIXED:
                    return ValueDouble.get(buff.getDouble());
            }
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(DataUtils.readVarLong(buff))));
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueDouble v = (ValueDouble) o;
        return Double.compare(value, v.value);
    }
}
