package com.glodon.base.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueDecimal extends Value {

    public static final ValueDecimal ZERO = new ValueDecimal(BigDecimal.ZERO);
    public static final ValueDecimal ONE = new ValueDecimal(BigDecimal.ONE);
    private final BigDecimal value;
    private String valueString;

    private ValueDecimal(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException();
        } else if (!value.getClass().equals(BigDecimal.class)) {
            throw UnificationException.get("invalid class %s.", value.getClass().getName());
        }
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.DECIMAL;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return value;
    }

    @Override
    public String getString() {
        if (valueString == null) {
            String p = value.toPlainString();
            if (p.length() < 40) {
                valueString = p;
            } else {
                valueString = value.toString();
            }
        }
        return valueString;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public Object getObject() {
        return value;
    }

    public static ValueDecimal get(BigDecimal dec) {
        if (BigDecimal.ZERO.equals(dec)) {
            return ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return ONE;
        }
        return new ValueDecimal(dec);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueDecimal && value.equals(((ValueDecimal) other).value);
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return DECIMAL;
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
            BigDecimal x = (BigDecimal) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            BigDecimal x = v.getBigDecimal();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, BigDecimal x) {
            if (BigDecimal.ZERO.equals(x)) {
                buff.put((byte) TAG_BIG_DECIMAL_0);
            } else if (BigDecimal.ONE.equals(x)) {
                buff.put((byte) TAG_BIG_DECIMAL_1);
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits < 64) {
                    if (scale == 0) {
                        buff.put((byte) TAG_BIG_DECIMAL_SMALL);
                    } else {
                        buff.put((byte) TAG_BIG_DECIMAL_SMALL_SCALED).putVarInt(scale);
                    }
                    buff.putVarLong(b.longValue());
                } else {
                    byte[] bytes = b.toByteArray();
                    buff.put((byte) DECIMAL).putVarInt(scale).putVarInt(bytes.length).put(bytes);
                }
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
                case TAG_BIG_DECIMAL_0:
                    return ZERO;
                case TAG_BIG_DECIMAL_1:
                    return ONE;
                case TAG_BIG_DECIMAL_SMALL:
                    return ValueDecimal.get(BigDecimal.valueOf(DataUtils.readVarLong(buff)));
                case TAG_BIG_DECIMAL_SMALL_SCALED:
                    int scale = DataUtils.readVarInt(buff);
                    return ValueDecimal.get(BigDecimal.valueOf(DataUtils.readVarLong(buff), scale));
            }
            int scale = DataUtils.readVarInt(buff);
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = DataUtils.newBytes(len);
            buff.get(bytes);
            BigInteger b = new BigInteger(bytes);
            return ValueDecimal.get(new BigDecimal(b, scale));
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueDecimal v = (ValueDecimal) o;
        return value.compareTo(v.value);
    }
}
