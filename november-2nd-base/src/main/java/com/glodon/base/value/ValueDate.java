package com.glodon.base.value;

import java.nio.ByteBuffer;
import java.sql.Date;

import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.DateTimeUtils;
import com.glodon.base.util.MathUtils;
import com.glodon.base.util.StringUtils;
import com.glodon.base.storage.DataBuffer;

public class ValueDate extends Value {

    public static final int DISPLAY_SIZE = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        this.dateValue = dateValue;
    }

    public long getDateValue() {
        return dateValue;
    }

    @Override
    public Date getDate() {
        return DateTimeUtils.convertDateValueToDate(dateValue);
    }

    @Override
    public int getType() {
        return Value.DATE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendDate(buff, dateValue);
        return buff.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate && dateValue == (((ValueDate) other).dateValue);
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    @Override
    public Object getObject() {
        return getDate();
    }

    protected static void appendDate(StringBuilder buff, long dateValue) {
        int y = DateTimeUtils.yearFromDateValue(dateValue);
        int m = DateTimeUtils.monthFromDateValue(dateValue);
        int d = DateTimeUtils.dayFromDateValue(dateValue);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return DATE;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Date a = (Date) aObj;
            Date b = (Date) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 40;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            Date d = (Date) obj;
            write0(buff, d.getTime());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            long d = ((ValueDate) v).getDateValue();
            write0(buff, d);
        }

        private void write0(DataBuffer buff, long v) {
            buff.put((byte) Value.DATE).putVarLong(v);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return new ValueDate(DataUtils.readVarLong(buff));
        }
    };

    @Override
    public int compareTo(Value o) {
        return MathUtils.compareLong(dateValue, ((ValueDate) o).dateValue);
    }
}
