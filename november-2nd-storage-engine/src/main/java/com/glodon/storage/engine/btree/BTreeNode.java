package com.glodon.storage.engine.btree;

import java.nio.ByteBuffer;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.util.DataUtils;


/**
 * BTree 行星节点实现
 * <p>
 * Created by liujing on 2023/10/16.
 */
public class BTreeNode extends LocalPage {

    private PageReference[] children;

    BTreeNode(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isNode() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return children == null || children.length == 0;
    }

    @Override
    public PageReference[] getChildren() {
        return children;
    }

    @Override
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        if (ref.page != null) {
            return ref.page;
        } else {
            Page p = bTreeMap.getBTreeStorage().readPage(ref.pos);
            ref.replacePage(p);
            p.setRef(ref);
            p.setParentRef(getRef());
            return p;
        }
    }

    @Override
    BTreeNode split(int at) {
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        BTreeNode newPage = create(bTreeMap, bKeys, bChildren, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public long getTotalCount() {
        long totalCount = 0;
        for (PageReference x : children) {
            if (x.page != null)
                totalCount += x.page.getTotalCount();
        }
        return totalCount;
    }

    @Override
    void setAndInsertChild(int index, PageOperations.TmpNodePage tmpNodePage) {
        children = children.clone(); // 必须弄一份新的，否则影响其他线程
        children[index] = tmpNodePage.right;
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = tmpNodePage.key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = tmpNodePage.left;
        children = newChildren;

        tmpNodePage.left.page.setParentRef(getRef());
        tmpNodePage.right.page.setParentRef(getRef());
        addMemory(bTreeMap.getKeyType().getMemory(tmpNodePage.key) + PageUtils.PAGE_MEMORY_CHILD);
    }

    @Override
    public void remove(int index) {
        if (keys.length > 0) // 删除最后一个children时，keys已经空了
            super.remove(index);
        addMemory(-PageUtils.PAGE_MEMORY_CHILD);
        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount - 1];
        DataUtils.copyExcept(children, newChildren, childCount, index);
        children = newChildren;
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
                     boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        children = new PageReference[keyLength + 1];
        long[] p = new long[keyLength + 1];
        for (int i = 0; i <= keyLength; i++) {
            p[i] = buff.getLong();
        }
        for (int i = 0; i <= keyLength; i++) {
            int pageType = buff.get();
            if (pageType == 0)
                buff.getInt(); // replicationHostIds
            children[i] = new PageReference(null, p[i]);
        }
        buff = expandPage(buff, type, start, pageLength);

        bTreeMap.getKeyType().read(buff, keys, keyLength);
        recalculateMemory();
    }

    private int write(Chunk chunk, DataBuffer buff) {
        int start = buff.position();
        int keyLength = keys.length;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        int type = PageUtils.PAGE_TYPE_NODE;
        buff.put((byte) type);
        writeChildrenPositions(buff);
        for (int i = 0; i <= keyLength; i++) {
            if (children[i].isLeafPage()) {
                buff.put((byte) 0);
                buff.putInt(0); // replicationHostIds
            } else {
                buff.put((byte) 1);
            }
        }
        int compressStart = buff.position();
        bTreeMap.getKeyType().write(buff, keys, keyLength);

        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        updateChunkAndCachePage(chunk, start, pageLength, type);

        bTreeMap.getBTreeStorage().cachePage(pos, this, getMemory());

        removeIfInMemory();
        return typePos + 1;
    }

    private void writeChildrenPositions(DataBuffer buff) {
        for (int i = 0, len = keys.length; i <= len; i++) {
            buff.putLong(children[i].pos);
        }
    }

    @Override
    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        if (pos != 0) {
            return;
        }
        int patch = write(chunk, buff);
        for (int i = 0, len = children.length; i < len; i++) {
            Page p = children[i].page;
            if (p != null) {
                p.writeUnsavedRecursive(chunk, buff);
                children[i].pos = p.pos;
            }
        }
        int old = buff.position();
        buff.position(patch);
        writeChildrenPositions(buff);
        buff.position(old);
    }

    @Override
    void writeEnd() {
        for (int i = 0, len = children.length; i < len; i++) {
            PageReference ref = children[i];
            if (ref.page != null) {
                if (ref.page.getPos() == 0) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                            "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos);
            }
        }
    }

    @Override
    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        mem += this.getRawChildPageCount() * PageUtils.PAGE_MEMORY_CHILD;
        addMemory(mem - memory);
    }

    @Override
    public BTreeNode copy() {
        return copy(true);
    }

    private BTreeNode copy(boolean removePage) {
        BTreeNode newPage = create(bTreeMap, keys, children, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.setParentRef(getParentRef());
        newPage.setRef(getRef());
        if (removePage) {
            removePage();
        }
        return newPage;
    }

    @Override
    public void removeAllRecursive() {
        if (children != null) {
            for (int i = 0, size = bTreeMap.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long pos = children[i].pos;
                    int type = PageUtils.getPageType(pos);
                    if (type == PageUtils.PAGE_TYPE_LEAF) {
                        Chunk c = bTreeMap.getBTreeStorage().getChunk(pos);
                        int mem = c.getPageLength(pos);
                        bTreeMap.getBTreeStorage().removePage(pos, mem);
                    } else {
                        bTreeMap.getBTreeStorage().readPage(pos).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    static BTreeNode create(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        BTreeNode p = new BTreeNode(map);
        p.keys = keys;
        p.children = children;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    @Override
    protected void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info) {
        if (children != null) {
            buff.append(indent).append("children: ").append(keys.length + 1).append('\n');
            for (int i = 0, len = keys.length; i <= len; i++) {
                buff.append('\n');
                if (children[i].page != null) {
                    children[i].page.getPrettyPageInfoRecursive(indent + "  ", info);
                } else {
                    if (info.readOffLinePage) {
                        bTreeMap.getBTreeStorage().readPage(children[i].pos)
                                .getPrettyPageInfoRecursive(indent + "  ", info);
                    } else {
                        buff.append(indent).append("  ");
                        buff.append("*** off-line *** ").append(children[i]).append('\n');
                    }
                }
            }
        }
    }
}
