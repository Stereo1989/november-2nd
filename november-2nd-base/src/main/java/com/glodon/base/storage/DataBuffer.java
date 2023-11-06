package com.glodon.base.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;

import com.glodon.base.Constants;
import com.glodon.base.storage.type.StorageDataType;
import com.glodon.base.storage.type.StorageDataTypeBase;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.MathUtils;
import com.glodon.base.value.Value;
import com.glodon.base.value.ValueBoolean;
import com.glodon.base.value.ValueByte;
import com.glodon.base.value.ValueDate;
import com.glodon.base.value.ValueDecimal;
import com.glodon.base.value.ValueDouble;
import com.glodon.base.value.ValueFloat;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueLong;
import com.glodon.base.value.ValueNull;
import com.glodon.base.value.ValueShort;
import com.glodon.base.value.ValueString;
import com.glodon.base.value.ValueStringFixed;
import com.glodon.base.value.ValueStringIgnoreCase;
import com.glodon.base.value.ValueTime;
import com.glodon.base.value.ValueTimestamp;
import com.glodon.base.value.ValueUuid;

/**
 * Created by liujing on 2023/10/12.
 */
public class DataBuffer implements AutoCloseable {

    public static final StorageDataTypeBase[] TYPES = new StorageDataTypeBase[Value.TYPE_COUNT];

    static {
        TYPES[Value.NULL] = ValueNull.type;
        TYPES[Value.BOOLEAN] = ValueBoolean.type;
        TYPES[Value.BYTE] = ValueByte.type;
        TYPES[Value.SHORT] = ValueShort.type;
        TYPES[Value.INT] = ValueInt.type;
        TYPES[Value.LONG] = ValueLong.type;
        TYPES[Value.FLOAT] = ValueFloat.type;
        TYPES[Value.DOUBLE] = ValueDouble.type;
        TYPES[Value.DECIMAL] = ValueDecimal.type;
        TYPES[Value.STRING] = ValueString.type;
        TYPES[Value.UUID] = ValueUuid.type;
        TYPES[Value.DATE] = ValueDate.type;
        TYPES[Value.TIME] = ValueTime.type;
        TYPES[Value.TIMESTAMP] = ValueTimestamp.type;
    }

    public static final int MAX_REUSE_CAPACITY = 4 * 1024 * 1024;
    private static final int MIN_GROW = 1024;

    private final DataHandler handler;
    private ByteBuffer reuse;
    private ByteBuffer buff;
    private boolean direct;

    public static DataBuffer create(int capacity) {
        return new DataBuffer(null, capacity);
    }

    protected DataBuffer() {
        this(null, MIN_GROW);
    }

    protected DataBuffer(DataHandler handler, int capacity) {
        this(handler, capacity, true);
    }

    protected DataBuffer(DataHandler handler, int capacity, boolean direct) {
        this.handler = handler;
        this.direct = direct;
        reuse = allocate(capacity);
        buff = reuse;
    }

    protected DataBuffer(ByteBuffer buff) {
        this.handler = null;
        this.buff = reuse = buff;
    }

    public DataHandler getHandler() {
        return handler;
    }

    public void reset() {
        buff.position(0);
    }


    public DataBuffer putVarInt(int x) {
        DataUtils.writeVarInt(ensureCapacity(5), x);
        return this;
    }

    public DataBuffer putVarLong(long x) {
        DataUtils.writeVarLong(ensureCapacity(10), x);
        return this;
    }

    public DataBuffer putStringData(String s, int len) {
        ByteBuffer b = ensureCapacity(3 * len);
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                b.put((byte) c);
            } else if (c >= 0x800) {
                b.put((byte) (0xe0 | (c >> 12)));
                b.put((byte) (((c >> 6) & 0x3f)));
                b.put((byte) (c & 0x3f));
            } else {
                b.put((byte) (0xc0 | (c >> 6)));
                b.put((byte) (c & 0x3f));
            }
        }
        return this;
    }

    public DataBuffer put(byte x) {
        ensureCapacity(1).put(x);
        return this;
    }

    public DataBuffer putChar(char x) {
        ensureCapacity(2).putChar(x);
        return this;
    }

    public DataBuffer putShort(short x) {
        ensureCapacity(2).putShort(x);
        return this;
    }

    public DataBuffer putInt(int x) {
        ensureCapacity(4).putInt(x);
        return this;
    }

    public int getInt() {
        return buff.getInt();
    }

    public short getUnsignedByte(int pos) {
        return (short) (buff.get(pos) & 0xff);
    }

    public DataBuffer slice(int start, int end) {
        int pos = buff.position();
        int limit = buff.limit();
        buff.position(start);
        buff.limit(end);
        ByteBuffer newBuffer = buff.slice();
        buff.position(pos);
        buff.limit(limit);
        return new DataBuffer(newBuffer);
    }

    public DataBuffer getBuffer(int start, int end) {
        byte[] bytes = new byte[end - start];
        // 不能直接这样用，get的javadoc是错的，start应该是bytes的位置
        // buff.get(bytes, start, end - start);
        int pos = buff.position();
        buff.position(start);
        buff.get(bytes, 0, end - start);
        buff.position(pos);
        ByteBuffer newBuffer = ByteBuffer.wrap(bytes);
        return new DataBuffer(newBuffer);
    }

    public void read(byte[] dst, int off, int len) {
        this.buff.get(dst, off, len);
    }

    public byte readByte() {
        return buff.get();
    }

    public void setPos(int pos) {
        buff.position(pos);
    }

    public DataBuffer putLong(long x) {
        ensureCapacity(8).putLong(x);
        return this;
    }

    public long getLong() {
        return buff.getLong();
    }

    public DataBuffer putFloat(float x) {
        ensureCapacity(4).putFloat(x);
        return this;
    }

    public DataBuffer putDouble(double x) {
        ensureCapacity(8).putDouble(x);
        return this;
    }

    public DataBuffer put(byte[] bytes) {
        ensureCapacity(bytes.length).put(bytes);
        return this;
    }

    public DataBuffer put(byte[] bytes, int offset, int length) {
        ensureCapacity(length).put(bytes, offset, length);
        return this;
    }

    public DataBuffer put(ByteBuffer src) {
        ensureCapacity(src.remaining()).put(src);
        return this;
    }

    public DataBuffer limit(int newLimit) {
        ensureCapacity(newLimit - buff.position()).limit(newLimit);
        return this;
    }

    public int capacity() {
        return buff.capacity();
    }

    public DataBuffer position(int newPosition) {
        buff.position(newPosition);
        return this;
    }

    public int limit() {
        return buff.limit();
    }

    public int position() {
        return buff.position();
    }

    public DataBuffer get(byte[] dst) {
        buff.get(dst);
        return this;
    }

    public DataBuffer putInt(int index, int value) {
        buff.putInt(index, value);
        return this;
    }

    public DataBuffer putShort(int index, short value) {
        buff.putShort(index, value);
        return this;
    }

    public DataBuffer putByte(int index, byte value) {
        buff.put(index, value);
        return this;
    }

    public DataBuffer clear() {
        if (buff.limit() > MAX_REUSE_CAPACITY) {
            buff = reuse;
        } else if (buff != reuse) {
            reuse = buff;
        }
        buff.clear();
        return this;
    }

    public ByteBuffer getBuffer() {
        return buff;
    }

    public ByteBuffer getAndFlipBuffer() {
        buff.flip();
        return buff;
    }

    public ByteBuffer getAndCopyBuffer() {
        buff.flip();
        ByteBuffer value = allocate(buff.limit());
        value.put(buff);
        value.flip();
        return value;
    }

    public ByteBuffer write(StorageDataType type, Object obj) {
        type.write(this, obj);
        ByteBuffer buff = getAndFlipBuffer();

        ByteBuffer v = allocate(buff.limit());
        v.put(buff);
        v.flip();
        return v;
    }

    public void checkCapacity(int plus) {
        ensureCapacity(plus);
    }

    private ByteBuffer ensureCapacity(int len) {
        if (buff.remaining() < len) {
            grow(len);
        }
        return buff;
    }

    private void grow(int additional) {
        int pos = buff.position();
        ByteBuffer temp = buff;
        int needed = additional - temp.remaining();
        long grow = Math.max(needed, MIN_GROW);
        grow = Math.max(temp.capacity() / 2, grow);
        int newCapacity = (int) Math.min(Integer.MAX_VALUE, temp.capacity() + grow);
        if (newCapacity < needed) {
            throw new OutOfMemoryError("Capacity: " + newCapacity + " needed: " + needed);
        }
        try {
            buff = allocate(newCapacity);
        } catch (OutOfMemoryError e) {
            throw new OutOfMemoryError("Capacity: " + newCapacity);
        }
        // temp.flip();
        temp.position(0);
        buff.put(temp);
        buff.position(pos);
        if (newCapacity <= MAX_REUSE_CAPACITY) {
            reuse = buff;
        }
    }

    public void fillAligned() {
        int position = buff.position();
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        int len = MathUtils.roundUpInt(position + 2, Constants.FILE_BLOCK_SIZE);
        ensureCapacity(len - position);
        buff.position(len);
    }

    public byte[] getBytes() {
        return buff.array();
    }

    public int length() {
        return buff.position();
    }

    public void writeValue(Value v) {
        writeValue(this, v);
    }

    private void writeValue(DataBuffer buff, Value v) {
        int type = v.getType();
        switch (type) {
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                buff.put((byte) type);
                writeString(buff, v.getString());
                break;
            default:
                type = StorageDataType.getTypeId(type);
                TYPES[type].writeValue(buff, v);
        }
    }

    private static void writeString(DataBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    public Value readValue() {
        return readValue(this.buff);
    }

    public static Value readValue(ByteBuffer buff) {
        int type = buff.get() & 255;
        switch (type) {
            case Value.STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(readString(buff));
            case Value.STRING_FIXED:
                return ValueStringFixed.get(readString(buff));
            default:
                int type2 = StorageDataType.getTypeId(type);
                return TYPES[type2].readValue(buff, type);
        }
    }

    private static int readVarInt(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    private static long readVarLong(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    private static String readString(ByteBuffer buff) {
        int len = readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

    @SuppressWarnings("resource")
    public static void copyString(Reader source, OutputStream target) throws IOException {
        char[] buff = new char[Constants.IO_BUFFER_SIZE];
        DataBuffer d = new DataBuffer(null, 3 * Constants.IO_BUFFER_SIZE, false);
        while (true) {
            int l = source.read(buff);
            if (l < 0) {
                break;
            }
            d.writeStringWithoutLength(buff, l);
            target.write(d.getBytes(), 0, d.length());
            d.reset();
        }
    }

    private void writeStringWithoutLength(char[] chars, int len) {
        int p = this.buff.position();
        byte[] buff = this.buff.array();
        for (int i = 0; i < len; i++) {
            int c = chars[i];
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        this.buff.position(p);
    }

    private ByteBuffer allocate(int capacity) {
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    @Override
    public void close() {
        if (factory != null) {
            factory.recycle(this);
        } else {
            DataBufferFactory.getConcurrentFactory().recycle(this);
        }
    }

    private DataBufferFactory factory;

    public void setFactory(DataBufferFactory factory) {
        this.factory = factory;
    }

    public static DataBuffer create() {
        return DataBufferFactory.getConcurrentFactory().create();
    }

    public static DataBuffer getOrCreate(int capacity) {
        return DataBufferFactory.getConcurrentFactory().create(capacity);
    }
}
