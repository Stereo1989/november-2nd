package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataTypeBase;

public class ValueBoolean extends Value {

    public static final int PRECISION = 1;

    public static final int DISPLAY_SIZE = 5;

    public static final ValueBoolean TRUE = new ValueBoolean(true);
    public static final ValueBoolean FALSE = new ValueBoolean(false);

    private final boolean value;

    private ValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.BOOLEAN;
    }

    @Override
    public String getString() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public boolean getBoolean() {
        return value;
    }

    @Override
    public int hashCode() {
        return value ? 1 : 0;
    }

    @Override
    public Object getObject() {
        return value;
    }

    public static ValueBoolean get(boolean b) {
        return b ? TRUE : FALSE;
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return TYPE_BOOLEAN;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Boolean a = (Boolean) aObj;
            Boolean b = (Boolean) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 0;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            write0(buff, ((Boolean) obj).booleanValue());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            write0(buff, v.getBoolean());
        }

        private void write0(DataBuffer buff, boolean v) {
            buff.put((byte) (v ? TAG_BOOLEAN_TRUE : TYPE_BOOLEAN));
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            if (tag == TYPE_BOOLEAN)
                return ValueBoolean.get(false);
            else
                return ValueBoolean.get(true);
        }
    };

    @Override
    public int compareTo(Value o) {
        boolean v2 = ((ValueBoolean) o).value;
        boolean v = value;
        return (v == v2) ? 0 : (v ? 1 : -1);
    }
}
