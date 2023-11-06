package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.MathUtils;

public class ValueByte extends Value {

    private final byte value;

    private ValueByte(byte value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.BYTE;
    }

    @Override
    public byte getByte() {
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
        return Byte.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueByte && value == ((ValueByte) other).value;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return BYTE;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Byte a = (Byte) aObj;
            Byte b = (Byte) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 0;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            buff.put((byte) Value.BYTE).put(((Byte) obj).byteValue());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            buff.put((byte) Value.BYTE).put(v.getByte());
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return new ValueByte(buff.get());
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueByte v = (ValueByte) o;
        return MathUtils.compareInt(value, v.value);
    }
}
