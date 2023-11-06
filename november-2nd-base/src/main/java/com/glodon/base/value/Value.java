package com.glodon.base.value;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class Value implements Comparable<Value> {

    public static final int UNKNOWN = -1;

    public static final int NULL = 0;

    public static final int BOOLEAN = 1;

    public static final int BYTE = 2;

    public static final int SHORT = 3;

    public static final int INT = 4;

    public static final int LONG = 5;

    public static final int DECIMAL = 6;

    public static final int DOUBLE = 7;

    public static final int FLOAT = 8;

    public static final int TIME = 9;

    public static final int DATE = 10;

    public static final int TIMESTAMP = 11;

    public static final int STRING = 12;

    public static final int STRING_IGNORECASE = 13;

    public static final int UUID = 14;

    public static final int STRING_FIXED = 15;

    public static final int TYPE_COUNT = STRING_FIXED + 1;

    public boolean getBoolean() {
        return false;
    }

    public byte getByte() {
        return 0;
    }

    public short getShort() {
        return 0;
    }

    public int getInt() {
        return 0;
    }

    public long getLong() {
        return 0;
    }

    public float getFloat() {
        return 0.0F;
    }

    public double getDouble() {
        return 0.0D;
    }

    public BigDecimal getBigDecimal() {
        return null;
    }

    public UUID getUuid() {
        return null;
    }

    public Date getDate() {
        return null;
    }

    public Time getTime() {
        return null;
    }

    public Timestamp getTimestamp() {
        return null;
    }

    public abstract int getType();

    public abstract String getString();

    public abstract Object getObject();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);
}
