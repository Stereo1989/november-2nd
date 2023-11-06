package com.glodon.base.util;

/**
 * A list of bits.
 */
public final class BitField {

    private static final int ADDRESS_BITS = 6;
    private static final int BITS = 64;
    private static final int ADDRESS_MASK = BITS - 1;
    private long[] data;
    private int maxLength;

    public BitField() {
        this(64);
    }

    public BitField(int capacity) {
        data = new long[capacity >>> 3];
    }

    public int nextClearBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        for (; i < max; i++) {
            if (data[i] == -1) {
                continue;
            }
            int j = Math.max(fromIndex, i << ADDRESS_BITS);
            for (int end = j + 64; j < end; j++) {
                if (!get(j)) {
                    return j;
                }
            }
        }
        return max << ADDRESS_BITS;
    }

    public boolean get(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return false;
        }
        return (data[addr] & getBitMask(i)) != 0;
    }

    public int getByte(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return 0;
        }
        return (int) (data[addr] >>> (i & (7 << 3)) & 255);
    }

    public void setByte(int i, int x) {
        int addr = i >> ADDRESS_BITS;
        checkCapacity(addr);
        data[addr] |= ((long) x) << (i & (7 << 3));
        if (maxLength < i && x != 0) {
            maxLength = i + 7;
        }
    }

    public void set(int i) {
        int addr = i >> ADDRESS_BITS;
        checkCapacity(addr);
        data[addr] |= getBitMask(i);
        if (maxLength < i) {
            maxLength = i;
        }
    }

    public void clear(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return;
        }
        data[addr] &= ~getBitMask(i);
    }

    private static long getBitMask(int i) {
        return 1L << (i & ADDRESS_MASK);
    }

    private void checkCapacity(int size) {
        if (size >= data.length) {
            expandCapacity(size);
        }
    }

    private void expandCapacity(int size) {
        while (size >= data.length) {
            int newSize = data.length == 0 ? 1 : data.length * 2;
            long[] d = new long[newSize];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    public void set(int fromIndex, int toIndex, boolean value) {
        for (int i = toIndex - 1; i >= fromIndex; i--) {
            set(i, value);
        }
        if (value) {
            if (toIndex > maxLength) {
                maxLength = toIndex;
            }
        } else {
            if (toIndex >= maxLength) {
                maxLength = fromIndex;
            }
        }
    }

    private void set(int i, boolean value) {
        if (value) {
            set(i);
        } else {
            clear(i);
        }
    }

    public int length() {
        int m = maxLength >> ADDRESS_BITS;
        while (m > 0 && data[m] == 0) {
            m--;
        }
        maxLength = (m << ADDRESS_BITS) + (64 - Long.numberOfLeadingZeros(data[m]));
        return maxLength;
    }
}
