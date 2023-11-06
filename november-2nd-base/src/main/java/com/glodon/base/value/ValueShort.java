package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.MathUtils;

public class ValueShort extends Value {

    private final short value;

    private ValueShort(short value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.SHORT;
    }

    @Override
    public short getShort() {
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
        return Short.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueShort && value == ((ValueShort) other).value;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return SHORT;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Short a = (Short) aObj;
            Short b = (Short) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 24;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            buff.put((byte) Value.SHORT).putShort(((Short) obj).shortValue());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            buff.put((byte) Value.SHORT).putShort(v.getShort());
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return new ValueShort(buff.getShort());
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueShort v = (ValueShort) o;
        return MathUtils.compareInt(value, v.value);
    }
}
