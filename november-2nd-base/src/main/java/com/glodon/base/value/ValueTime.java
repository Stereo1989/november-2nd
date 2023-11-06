package com.glodon.base.value;

import java.nio.ByteBuffer;
import java.sql.Time;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.DateTimeUtils;
import com.glodon.base.util.MathUtils;
import com.glodon.base.util.StringUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueTime extends Value {

    static final int DISPLAY_SIZE = 8;

    private final long nanos;

    private ValueTime(long nanos) {
        this.nanos = nanos;
    }

    public static ValueTime fromNanos(long nanos) {
        return new ValueTime(nanos);
    }

    public static ValueTime get(Time time) {
        return fromNanos(DateTimeUtils.nanosFromDate(time.getTime()));
    }

    public long getNanos() {
        return nanos;
    }

    @Override
    public Time getTime() {
        return DateTimeUtils.convertNanoToTime(nanos);
    }

    @Override
    public int getType() {
        return Value.TIME;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendTime(buff, nanos, false);
        return buff.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueTime && nanos == (((ValueTime) other).nanos);
    }

    @Override
    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    @Override
    public Object getObject() {
        return getTime();
    }

    static void appendTime(StringBuilder buff, long nanos, boolean alwaysAddMillis) {
        if (nanos < 0) {
            buff.append('-');
            nanos = -nanos;
        }
        long ms = nanos / 1000000;
        nanos -= ms * 1000000;
        long s = ms / 1000;
        ms -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (alwaysAddMillis || ms > 0 || nanos > 0) {
            buff.append('.');
            int start = buff.length();
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (nanos > 0) {
                StringUtils.appendZeroPadded(buff, 6, nanos);
            }
            for (int i = buff.length() - 1; i > start; i--) {
                if (buff.charAt(i) != '0') {
                    break;
                }
                buff.deleteCharAt(i);
            }
        }
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return TIME;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Time a = (Time) aObj;
            Time b = (Time) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 40;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            Time t = (Time) obj;
            writeValue(buff, ValueTime.get(t));
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            ValueTime t = (ValueTime) v;
            long nanos = t.getNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put((byte) TIME).putVarLong(millis).putVarLong(nanos);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            long nanos = DataUtils.readVarLong(buff) * 1000000 + DataUtils.readVarLong(buff);
            return ValueTime.fromNanos(nanos);
        }
    };

    @Override
    public int compareTo(Value o) {
        return MathUtils.compareLong(nanos, ((ValueTime) o).nanos);
    }
}
