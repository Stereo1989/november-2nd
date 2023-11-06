package com.glodon.storage.engine.btree;

public class PageUtils {

    public static final int PAGE_TYPE_LEAF = 0;

    public static final int PAGE_TYPE_NODE = 1;

    public static final int PAGE_TYPE_COLUMN = 2;

    public static final int PAGE_COMPRESSED = 2;

    public static final int PAGE_COMPRESSED_HIGH = 2 + 4;

    public static final int PAGE_MEMORY = 128;

    public static final int PAGE_MEMORY_CHILD = 16;

    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 34);
    }

    public static int getPageOffset(long pos) {
        return (int) (pos >> 2);
    }

    public static int getPageType(long pos) {
        return ((int) pos) & 3;
    }

    public static long getPagePos(int chunkId, int offset, int type) {
        long pos = (long) chunkId << 34;
        pos |= (long) offset << 2;
        pos |= type;
        return pos;
    }

    public static boolean isLeafPage(long pos) {
        return pos > 0 && getPageType(pos) == PAGE_TYPE_LEAF;
    }

    public static boolean isNodePage(long pos) {
        return pos > 0 && getPageType(pos) == PAGE_TYPE_NODE;
    }
}
