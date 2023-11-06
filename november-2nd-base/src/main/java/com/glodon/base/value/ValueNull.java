package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.storage.DataBuffer;

public class ValueNull extends Value {

    public static final ValueNull INSTANCE = new ValueNull();

    private ValueNull() {
    }

    @Override
    public int getType() {
        return Value.NULL;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return NULL;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == null && bObj == null) {
                return 0;
            } else if (aObj == null) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public int getMemory(Object obj) {
            return 0;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            buff.put((byte) Value.NULL);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            buff.put((byte) Value.NULL);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return ValueNull.INSTANCE;
        }
    };

    @Override
    public int compareTo(Value o) {
        if (this == ValueNull.INSTANCE) {
            return o == ValueNull.INSTANCE ? 0 : -1;
        } else if (o == ValueNull.INSTANCE) {
            return 1;
        } else {
            return 0;
        }
    }
}
