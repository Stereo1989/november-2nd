package com.glodon.storage.engine.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

public class ChunkCompactor {

    private final BTreeStore btreeStore;
    private final ChunkManager chunkManager;

    public ChunkCompactor(BTreeStore btreeStore, ChunkManager chunkManager) {
        this.btreeStore = btreeStore;
        this.chunkManager = chunkManager;
    }

    public void executeCompact() {
        TreeSet<Long> removedPages = chunkManager.getRemovedPagesCopy();
        if (removedPages.isEmpty())
            return;
        List<Chunk> chunks = readChunks(removedPages);
        List<Chunk> unusedChunks = findUnusedChunks(chunks, removedPages);
        if (!unusedChunks.isEmpty()) {
            removeUnusedChunks(unusedChunks, removedPages);
            chunks.removeAll(unusedChunks);
        }
        rewrite(chunks, removedPages);
    }

    private List<Chunk> readChunks(TreeSet<Long> removedPages) {
        HashSet<Integer> chunkIds = new HashSet<>();
        for (Long pagePos : removedPages) {
            if (!PageUtils.isNodePage(pagePos))
                chunkIds.add(PageUtils.getPageChunkId(pagePos));
        }
        return chunkManager.readChunks(chunkIds);
    }

    private List<Chunk> findUnusedChunks(List<Chunk> chunks, TreeSet<Long> removedPages) {
        ArrayList<Chunk> unusedChunks = new ArrayList<>();
        for (Chunk c : chunks) {
            c.sumOfLivePageLength = 0;
            boolean unused = true;
            for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                if (!removedPages.contains(e.getKey())) {
                    c.sumOfLivePageLength += e.getValue();
                    unused = false;
                }
            }
            if (unused)
                unusedChunks.add(c);
        }
        return unusedChunks;
    }

    private void removeUnusedChunks(List<Chunk> unusedChunks, TreeSet<Long> removedPages) {
        if (removedPages.isEmpty())
            return;
        int size = removedPages.size();
        for (Chunk c : unusedChunks) {
            chunkManager.removeUnusedChunk(c);
            removedPages.removeAll(c.pagePositionToLengthMap.keySet());
        }
        if (size > removedPages.size()) {
            chunkManager.updateRemovedPages(removedPages);
        }
    }

    private void rewrite(List<Chunk> chunks, TreeSet<Long> removedPages) {
        if (btreeStore.getMinFillRate() <= 0 || removedPages.isEmpty()) {
            return;
        }

        List<Chunk> old = getRewritableChunks(chunks);
        boolean saveIfNeeded = false;
        for (Chunk c : old) {
            for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                long pos = e.getKey();
                if (PageUtils.isLeafPage(pos) && !removedPages.contains(pos)) {
                    Page p = btreeStore.readPage(pos);
                    p.markDirtyRecursive(); // 直接标记为脏页即可，不用更新元素
                    saveIfNeeded = true;
                }
            }
        }
        if (saveIfNeeded) {
            btreeStore.executeSave();
            removedPages = chunkManager.getRemovedPagesCopy();
            removeUnusedChunks(old, removedPages);
        }
    }

    private List<Chunk> getRewritableChunks(List<Chunk> chunks) {
        int minFillRate = btreeStore.getMinFillRate();
        List<Chunk> old = new ArrayList<>();
        for (Chunk c : chunks) {
            if (c.getFillRate() > minFillRate) {
                continue;
            } else {
                old.add(c);
            }
        }
        if (old.isEmpty()) {
            return old;
        }

        Collections.sort(old, (o1, o2) -> {
            long comp = o1.getFillRate() - o2.getFillRate();
            if (comp == 0) {
                comp = o1.sumOfLivePageLength - o2.sumOfLivePageLength;
            }
            return Long.signum(comp);
        });

        long bytes = 0;
        int index = 0;
        int size = old.size();
        long maxBytesToWrite = Chunk.MAX_SIZE;
        for (; index < size; index++) {
            bytes += old.get(index).sumOfLivePageLength;
            if (bytes > maxBytesToWrite) {
                break;
            }
        }
        return index == size ? old : old.subList(0, index + 1);
    }
}
