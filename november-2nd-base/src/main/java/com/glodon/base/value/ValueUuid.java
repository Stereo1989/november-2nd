package com.glodon.base.value;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.MathUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueUuid extends Value {

    private final long high, low;

    private ValueUuid(long high, long low) {
        this.high = high;
        this.low = low;
    }

    public static ValueUuid get(long high, long low) {
        return new ValueUuid(high, low);
    }

    @Override
    public int getType() {
        return Value.UUID;
    }

    private static void appendHex(StringBuilder buff, long x, int bytes) {
        for (int i = bytes * 8 - 4; i >= 0; i -= 8) {
            buff.append(Integer.toHexString((int) (x >> i) & 0xf))
                    .append(Integer.toHexString((int) (x >> (i - 4)) & 0xf));
        }
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(36);
        appendHex(buff, high >> 32, 4);
        buff.append('-');
        appendHex(buff, high >> 16, 2);
        buff.append('-');
        appendHex(buff, high, 2);
        buff.append('-');
        appendHex(buff, low >> 48, 2);
        buff.append('-');
        appendHex(buff, low, 6);
        return buff.toString();
    }

    int compare(Value o) {
        if (o == this) {
            return 0;
        }
        ValueUuid v = (ValueUuid) o;
        if (high == v.high) {
            return MathUtils.compareLong(low, v.low);
        }
        return high > v.high ? 1 : -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueUuid uuid = (ValueUuid) o;
        return high == uuid.high && low == uuid.low;
    }

    @Override
    public int hashCode() {
        return Objects.hash(high, low);
    }

//    @Override
//    public int hashCode() {
//        return (int) ((high >>> 32) ^ high ^ (low >>> 32) ^ low);
//    }
//
//    @Override
//    public boolean equals(Object other) {
//        return other instanceof ValueUuid && compare((Value) other) == 0;
//    }

    @Override
    public Object getObject() {
        return new UUID(high, low);
    }

    @Override
    public UUID getUuid() {
        return new UUID(high, low);
    }

    public byte[] getBytes() {
        byte[] buff = new byte[16];
        for (int i = 0; i < 8; i++) {
            buff[i] = (byte) ((high >> (8 * (7 - i))) & 255);
            buff[8 + i] = (byte) ((low >> (8 * (7 - i))) & 255);
        }
        return buff;
    }

    public long getHigh() {
        return high;
    }

    public long getLow() {
        return low;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return UUID;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            UUID a = (UUID) aObj;
            UUID b = (UUID) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 40;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            UUID a = (UUID) obj;
            write0(buff, a.getMostSignificantBits(), a.getLeastSignificantBits());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            ValueUuid uuid = (ValueUuid) v;
            write0(buff, uuid.getHigh(), uuid.getLow());
        }

        private void write0(DataBuffer buff, long high, long low) {
            buff.put((byte) UUID).putLong(high).putLong(low);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return new ValueUuid(buff.getLong(), buff.getLong());
        }
    };

    @Override
    public int compareTo(Value o) {
        if (o == this) {
            return 0;
        }
        ValueUuid v = (ValueUuid) o;
        if (high == v.high) {
            return MathUtils.compareLong(low, v.low);
        }
        return high > v.high ? 1 : -1;
    }
}
