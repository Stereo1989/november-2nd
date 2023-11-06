package com.glodon.base.value;

import java.nio.ByteBuffer;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.StringUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueString extends Value {

    private static final ValueString EMPTY = new ValueString("");
    protected final String value;

    protected ValueString(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString && value.equals(((ValueString) other).value);
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int getType() {
        return Value.STRING;
    }

    public static ValueString get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        return new ValueString(StringUtils.cache(s));
    }

    protected ValueString getNew(String s) {
        return ValueString.get(s);
    }

    public static final StringDataType type = new StringDataType();

    @Override
    public int compareTo(Value o) {
        ValueString v = (ValueString) o;
        return value.compareTo(v.value);
    }

    public static final class StringDataType extends StorageDataTypeBase {

        private StringDataType() {
        }

        @Override
        public int getType() {
            return STRING;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            return aObj.toString().compareTo(bObj.toString());
        }

        @Override
        public int getMemory(Object obj) {
            return 24 + 2 * obj.toString().length();
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            String s = (String) obj;
            write0(buff, s);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            String s = v.getString();
            write0(buff, s);
        }

        private void write0(DataBuffer buff, String s) {
            if (s == null) {
                buff.put((byte) STRING).putVarInt((byte) 0);
                return;
            }
            int len = s.length();
            if (len <= 15) {
                buff.put((byte) (TAG_STRING_0_15 + len));
            } else {
                buff.put((byte) STRING).putVarInt(len);
            }
            buff.putStringData(s, len);
        }

        @Override
        public String read(ByteBuffer buff) {
            return (String) super.read(buff);
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            int len;
            if (tag == STRING) {
                len = DataUtils.readVarInt(buff);
                if (len == 0)
                    return ValueNull.INSTANCE;
            } else {
                len = tag - TAG_STRING_0_15;
            }
            return ValueString.get(DataUtils.readString(buff, len));
        }
    }
}
