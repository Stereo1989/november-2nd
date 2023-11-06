package com.glodon.base.value;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.DateTimeUtils;
import com.glodon.base.util.MathUtils;

public class ValueTimestamp extends Value {

    static final int DISPLAY_SIZE = 23;
    private final long dateValue;
    private final long nanos;

    private ValueTimestamp(long dateValue, long nanos) {
        this.dateValue = dateValue;
        this.nanos = nanos;
    }

    public static ValueTimestamp fromDateValueAndNanos(long dateValue, long nanos) {
        return new ValueTimestamp(dateValue, nanos);
    }

    public static ValueTimestamp get(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long nanos = timestamp.getNanos() % 1000000;
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        nanos += DateTimeUtils.nanosFromDate(ms);
        return fromDateValueAndNanos(dateValue, nanos);
    }

    public long getDateValue() {
        return dateValue;
    }

    public long getNanos() {
        return nanos;
    }

    @Override
    public Timestamp getTimestamp() {
        return DateTimeUtils.convertDateValueToTimestamp(dateValue, nanos);
    }

    @Override
    public int getType() {
        return Value.TIMESTAMP;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        ValueDate.appendDate(buff, dateValue);
        buff.append(' ');
        ValueTime.appendTime(buff, nanos, true);
        return buff.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestamp)) {
            return false;
        }
        ValueTimestamp x = (ValueTimestamp) other;
        return dateValue == x.dateValue && nanos == x.nanos;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ nanos ^ (nanos >>> 32));
    }

    @Override
    public Object getObject() {
        return getTimestamp();
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return TIMESTAMP;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Timestamp a = (Timestamp) aObj;
            Timestamp b = (Timestamp) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 40;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            Timestamp t = (Timestamp) obj;
            writeValue(buff, ValueTimestamp.get(t));
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            ValueTimestamp ts = (ValueTimestamp) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put((byte) TIMESTAMP).putVarLong(dateValue).putVarLong(millis).putVarLong(nanos);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            long dateValue = DataUtils.readVarLong(buff);
            long nanos = DataUtils.readVarLong(buff) * 1000000 + DataUtils.readVarLong(buff);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
        }
    };

    @Override
    public int compareTo(Value o) {
        ValueTimestamp t = (ValueTimestamp) o;
        int c = MathUtils.compareLong(dateValue, t.dateValue);
        if (c != 0) {
            return c;
        }
        return MathUtils.compareLong(nanos, t.nanos);
    }
}
