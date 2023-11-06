package com.glodon.storage.engine.btree;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.compress.Compressor;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.util.DataUtils;
import com.glodon.base.fs.FileStorage;

/**
 * 数据叶基类
 * <p>
 * Created by liujing on 2023/10/16.
 */
public class Page {

    public static final Object[] NULL = new Object[0];
    protected final BTreeMap<?, ?> bTreeMap;
    protected long pos;
    private PageReference ref;
    private PageReference parentRef;

    protected Page(BTreeMap<?, ?> bTreeMap) {
        this.bTreeMap = bTreeMap;
    }

    public void setParentRef(PageReference parentRef) {
        this.parentRef = parentRef;
    }

    public PageReference getParentRef() {
        return parentRef;
    }

    public void setRef(PageReference ref) {
        this.ref = ref;
    }

    public PageReference getRef() {
        return ref;
    }

    public long getPos() {
        return pos;
    }

    private static RuntimeException ie() {
        return UnificationException.throwInternalError();
    }

    public Object getKey(int index) {
        throw ie();
    }

    public int getKeyCount() {
        throw ie();
    }

    public Object getValue(int index) {
        throw ie();
    }

    public Object getValue(int index, int[] columnIndexes) {
        throw ie();
    }

    public Object getValue(int index, boolean allColumns) {
        throw ie();
    }

    public boolean isEmpty() {
        throw ie();
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    public long getTotalCount() {
        return 0;
    }

    public Page getChildPage(int index) {
        throw ie();
    }

    public boolean isLeaf() {
        return false;
    }

    public boolean isNode() {
        return false;
    }

    public int binarySearch(Object key) {
        throw ie();
    }

    public int getPageIndex(Object key) {
        int index = binarySearch(key);
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        return index;
    }

    boolean needSplit() {
        throw ie();
    }

    Page split(int at) {
        throw ie();
    }

    public void setKey(int index, Object key) {
        throw ie();
    }

    public Object setValue(int index, Object value) {
        throw ie();
    }

    void setAndInsertChild(int index, PageOperations.TmpNodePage tmpNodePage) {
        throw ie();
    }

    public Page copyLeaf(int index, Object key, Object value) {
        throw ie();
    }

    public void remove(int index) {
        throw ie();
    }

    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                     boolean disableCheck) {
        throw ie();
    }

    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        throw ie();
    }

    void writeEnd() {
    }

    public int getRawChildPageCount() {
        return 0;
    }

    public int getMemory() {
        return 0;
    }

    public Page copy() {
        throw ie();
    }

    public void removePage() {
        throw ie();
    }

    void markDirty() {
        markDirty(false);
    }

    void markDirty(boolean hasUnsavedChanges) {
        if (pos != 0) {
            removePage();
            pos = 0;
        } else {
            if (hasUnsavedChanges) {
                bTreeMap.getBTreeStorage().setUnsavedChanges(true);
            }
        }
    }

    public void markDirtyRecursive() {
        markDirty(true);
        PageReference parentRef = getParentRef();
        while (parentRef != null) {
            parentRef.page.markDirty(false);
            parentRef = parentRef.page.getParentRef();
        }
    }

    public void removeAllRecursive() {
        throw ie();
    }

    public static Page read(BTreeMap<?, ?> map, FileStorage fileStorage, long pos, long filePos,
                            int pageLength) {
        ByteBuffer buff = readPageBuff(fileStorage, filePos, pageLength);
        int type = PageUtils.getPageType(pos);
        Page p = create(map, type);
        p.pos = pos;
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, pageLength, false);
        return p;
    }

    private static ByteBuffer readPageBuff(FileStorage fileStorage, long filePos, int pageLength) {
        if (pageLength < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1} ", pageLength, filePos);
        }
        return fileStorage.readFully(filePos, pageLength);
    }

    private static Page create(BTreeMap<?, ?> map, int type) {
        Page p;
        if (type == PageUtils.PAGE_TYPE_LEAF)
            p = new BTreeLeaf(map);
        else if (type == PageUtils.PAGE_TYPE_NODE)
            p = new BTreeNode(map);
        else if (type == PageUtils.PAGE_TYPE_COLUMN)
            p = new ColumnPage(map);
        else
            throw UnificationException.getInternalError("type: " + type);
        return p;
    }


    public Page gotoLeafPage(Object key) {
        return gotoLeafPage(key, false);
    }

    public Page gotoLeafPage(Object key, boolean markDirty) {
        Page p = this;
        while (p.isNode()) {
            if (markDirty) {
                p.markDirty();
            }
            int index = p.getPageIndex(key);
            p = p.getChildPage(index);
        }
        return p;
    }

    public PageReference[] getChildren() {
        throw ie();
    }

    static void readCheckValue(ByteBuffer buff, int chunkId, int offset, int pageLength,
                               boolean disableCheck) {
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (!disableCheck && check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest,
                    check);
        }
    }

    static void writeCheckValue(DataBuffer buff, int chunkId, int start, int pageLength, int checkPos) {
        int check = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putShort(checkPos, (short) check);
    }

    static void checkPageLength(int chunkId, int pageLength, int expectedPageLength) {
        if (pageLength != expectedPageLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId,
                    expectedPageLength, pageLength);
        }
    }

    void compressPage(DataBuffer buff, int compressStart, int type, int typePos) {
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            BTreeStore storage = bTreeMap.getBTreeStorage();
            int compressionLevel = storage.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = storage.getCompressorFast();
                    compressType = PageUtils.PAGE_COMPRESSED;
                } else {
                    compressor = storage.getCompressorHigh();
                    compressType = PageUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).put((byte) (type + compressType));
                    buff.position(compressStart).putVarInt(expLen - compLen).put(comp, 0, compLen);
                }
            }
        }
    }

    ByteBuffer expandPage(ByteBuffer buff, int type, int start, int pageLength) {
        boolean compressed = (type & PageUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & PageUtils.PAGE_COMPRESSED_HIGH) == PageUtils.PAGE_COMPRESSED_HIGH) {
                compressor = bTreeMap.getBTreeStorage().getCompressorHigh();
            } else {
                compressor = bTreeMap.getBTreeStorage().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            ByteBuffer newBuff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, newBuff.array(), newBuff.arrayOffset(), l);
            return newBuff;
        }
        return buff;
    }

    void updateChunkAndCachePage(Chunk chunk, int start, int pageLength, int type) {
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = PageUtils.getPagePos(chunk.id, start, type);
        chunk.pagePositionToLengthMap.put(pos, pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        bTreeMap.getBTreeStorage().cachePage(pos, this, getMemory());

        if (chunk.sumOfPageLength > Chunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", Chunk.MAX_SIZE,
                    chunk.sumOfPageLength);
    }

    public String getPrettyPageInfo(boolean readOffLinePage) {
        throw ie();
    }

    void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
    }

    static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }
}
