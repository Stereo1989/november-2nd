package com.glodon.base.compress;

import java.nio.ByteBuffer;

public final class CompressLZF implements Compressor {

    private static final int HASH_SIZE = 1 << 14;

    private static final int MAX_LITERAL = 1 << 5;

    private static final int MAX_OFF = 1 << 13;

    private static final int MAX_REF = (1 << 8) + (1 << 3);

    private int[] cachedHashTable;

    @Override
    public void setOptions(String options) {
    }

    private static int first(byte[] in, int inPos) {
        return (in[inPos] << 8) | (in[inPos + 1] & 255);
    }

    private static int first(ByteBuffer in, int inPos) {
        return (in.get(inPos) << 8) | (in.get(inPos + 1) & 255);
    }

    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) | (in[inPos + 2] & 255);
    }

    private static int next(int v, ByteBuffer in, int inPos) {
        return (v << 8) | (in.get(inPos + 2) & 255);
    }

    private static int hash(int h) {
        return ((h * 2777) >> 9) & (HASH_SIZE - 1);
    }

    @Override
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in[inPos + 2];
            // next
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            // if (ref < inPos
            // && ref > 0
            // && (off = inPos - ref - 1) < MAX_OFF
            // && in[ref + 2] == p2
            // && (((in[ref] & 255) << 8) | (in[ref + 1] & 255)) ==
            // ((future >> 8) & 0xffff)) {
            if (ref < inPos && ref > 0 && (off = inPos - ref - 1) < MAX_OFF && in[ref + 2] == p2
                    && in[ref + 1] == (byte) (future >> 8) && in[ref] == (byte) (future >> 16)) {
                // match
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                out[outPos++] = in[inPos++];
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        while (inPos < inLen) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    public int compress(ByteBuffer in, byte[] out, int outPos) {
        int inPos = in.position();
        int inLen = in.capacity() - inPos;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in.get(inPos + 2);
            // next
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            // if (ref < inPos
            // && ref > 0
            // && (off = inPos - ref - 1) < MAX_OFF
            // && in[ref + 2] == p2
            // && (((in[ref] & 255) << 8) | (in[ref + 1] & 255)) ==
            // ((future >> 8) & 0xffff)) {
            if (ref < inPos && ref > 0 && (off = inPos - ref - 1) < MAX_OFF && in.get(ref + 2) == p2
                    && in.get(ref + 1) == (byte) (future >> 8) && in.get(ref) == (byte) (future >> 16)) {
                // match
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in.get(ref + len) == in.get(inPos + len)) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                out[outPos++] = in.get(inPos++);
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        while (inPos < inLen) {
            out[outPos++] = in.get(inPos++);
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    @Override
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        // if ((inPos | outPos | outLen) < 0) {
        if (inPos < 0 || outPos < 0 || outLen < 0) {
            throw new IllegalArgumentException();
        }
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) {
                // literal run of length = ctrl + 1,
                ctrl++;
                // copy to output and move forward this many bytes
                // while (ctrl-- > 0) {
                // out[outPos++] = in[inPos++];
                // }
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                // back reference
                // the highest 3 bits are the match length
                int len = ctrl >> 5;
                // if the length is maxed, add the next byte to the length
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                // minimum back-reference is 3 bytes,
                // so 2 was subtracted before storing size
                len += 2;

                // ctrl is now the offset for a back-reference...
                // the logical AND operation removes the length bits
                ctrl = -((ctrl & 0x1f) << 8) - 1;

                // the next byte augments/increases the offset
                ctrl -= in[inPos++] & 255;

                // copy the back-reference bytes from the given
                // location in output to current position
                ctrl += outPos;
                if (outPos + len >= out.length) {
                    // reduce array bounds checking
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }

    public static void expand(ByteBuffer in, ByteBuffer out) {
        do {
            int ctrl = in.get() & 255;
            if (ctrl < MAX_LITERAL) {
                // literal run of length = ctrl + 1,
                ctrl++;
                // copy to output and move forward this many bytes
                // (maybe slice would be faster)
                for (int i = 0; i < ctrl; i++) {
                    out.put(in.get());
                }
            } else {
                // back reference
                // the highest 3 bits are the match length
                int len = ctrl >> 5;
                // if the length is maxed, add the next byte to the length
                if (len == 7) {
                    len += in.get() & 255;
                }
                // minimum back-reference is 3 bytes,
                // so 2 was subtracted before storing size
                len += 2;

                // ctrl is now the offset for a back-reference...
                // the logical AND operation removes the length bits
                ctrl = -((ctrl & 0x1f) << 8) - 1;

                // the next byte augments/increases the offset
                ctrl -= in.get() & 255;

                // copy the back-reference bytes from the given
                // location in output to current position
                // (maybe slice would be faster)
                ctrl += out.position();
                for (int i = 0; i < len; i++) {
                    out.put(out.get(ctrl++));
                }
            }
        } while (out.position() < out.capacity());
    }

    @Override
    public int getAlgorithm() {
        return LZF;
    }

}