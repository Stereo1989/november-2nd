package com.glodon.base.storage.type;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;
import com.glodon.base.value.Value;

/**
 * Created by liujing on 2023/10/12.
 */
public class BigIntegerType extends StorageDataTypeBase {

    @Override
    public int getType() {
        return TYPE_BIG_INTEGER;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        BigInteger a = (BigInteger) aObj;
        BigInteger b = (BigInteger) bObj;
        return a.compareTo(b);
    }

    @Override
    public int getMemory(Object obj) {
        return 100;
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        BigInteger x = (BigInteger) obj;
        if (BigInteger.ZERO.equals(x)) {
            buff.put((byte) TAG_BIG_INTEGER_0);
        } else if (BigInteger.ONE.equals(x)) {
            buff.put((byte) TAG_BIG_INTEGER_1);
        } else {
            int bits = x.bitLength();
            if (bits <= 63) {
                buff.put((byte) TAG_BIG_INTEGER_SMALL).putVarLong(x.longValue());
            } else {
                byte[] bytes = x.toByteArray();
                buff.put((byte) TYPE_BIG_INTEGER).putVarInt(bytes.length).put(bytes);
            }
        }
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        switch (tag) {
            case TAG_BIG_INTEGER_0:
                return BigInteger.ZERO;
            case TAG_BIG_INTEGER_1:
                return BigInteger.ONE;
            case TAG_BIG_INTEGER_SMALL:
                return BigInteger.valueOf(DataUtils.readVarLong(buff));
        }
        int len = DataUtils.readVarInt(buff);
        byte[] bytes = DataUtils.newBytes(len);
        buff.get(bytes);
        return new BigInteger(bytes);
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

}
