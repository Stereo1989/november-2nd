package com.glodon.storage.engine.btree;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;

import com.glodon.base.storage.DataBuffer;
import com.glodon.base.compress.CompressDeflate;
import com.glodon.base.compress.CompressLZF;
import com.glodon.base.compress.Compressor;
import com.glodon.base.util.DataUtils;
import com.glodon.base.storage.cache.CacheLongKeyLIRS;
import com.glodon.base.fs.FileStorage;
import com.glodon.base.fs.FileUtils;

public final class BTreeStore {

    private final BTreeMap<?, ?> bTreeMap;
    private final String mapBaseDir;
    private final ChunkManager chunkManager;
    private final int pageSplitSize;
    private final int minFillRate;

    private final UncaughtExceptionHandler backgroundExceptionHandler;
    private final CacheLongKeyLIRS<Page> cache;
    private final int compressionLevel;
    private Compressor compressorFast;
    private Compressor compressorHigh;

    private boolean closed;
    private volatile boolean hasUnsavedChanges;

    BTreeStore(BTreeMap<?, ?> bTreeMap) {
        this.bTreeMap = bTreeMap;
        pageSplitSize = bTreeMap.getConfig().getStoragePageSplitSize();
        this.minFillRate = bTreeMap.getConfig().getStorageMinFillRate();
        this.compressionLevel = bTreeMap.getConfig().getCompressionLevel();
        this.backgroundExceptionHandler = bTreeMap.getConfig().getStorageBackgroundExceptionHandler();
        this.chunkManager = new ChunkManager(this);
        if (bTreeMap.isInMemory()) {
            cache = null;
            mapBaseDir = null;
            return;
        }

        int cacheSize = bTreeMap.getConfig().getStorageCacheSize();
        if (cacheSize > 0) {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = cacheSize;
            cache = new CacheLongKeyLIRS<>(cc);
        } else {
            cache = null;
        }
        mapBaseDir = bTreeMap.getStorage().getStoragePath() + File.separator + bTreeMap.getName();
        if (!FileUtils.exists(mapBaseDir)) {
            FileUtils.createDirectories(mapBaseDir);
        } else {
            chunkManager.init(mapBaseDir);
        }
    }

    public IllegalStateException panic(int errorCode, String message, Object... arguments) {
        IllegalStateException e = DataUtils.newIllegalStateException(errorCode, message, arguments);
        return panic(e);
    }

    public IllegalStateException panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        closeNow();
        return e;
    }

    Chunk getLastChunk() {
        return chunkManager.getLastChunk();
    }

    public Chunk getChunk(long pos) {
        return chunkManager.getChunk(pos);
    }

    public void cachePage(long pos, Page page, int memory) {
        if (cache != null) {
            cache.put(pos, page, memory);
        }
    }

    public Page readPage(long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        return readLocalPage(pos);
    }

    private Page getPageFromCache(long pos) {
        return cache == null ? null : cache.get(pos);
    }

    private Page readLocalPage(long pos) {
        Page p = getPageFromCache(pos);
        if (p != null)
            return p;
        Chunk c = getChunk(pos);
        long filePos = Chunk.getFilePos(PageUtils.getPageOffset(pos));
        int pageLength = c.getPageLength(pos);
        p = Page.read(bTreeMap, c.fileStorage, pos, filePos, pageLength);
        cachePage(pos, p, p.getMemory());
        return p;
    }

    public void removePage(long pos, int memory) {
        hasUnsavedChanges = true;
        if (pos == 0) {
            return;
        }
        chunkManager.addRemovedPage(pos);
        if (cache != null) {
            if (PageUtils.isLeafPage(pos)) {
                cache.remove(pos);
            }
        }
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    public Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    public int getMinFillRate() {
        return minFillRate;
    }

    synchronized void remove() {
        closeNow();
        if (bTreeMap.isInMemory()) {
            return;
        }
        FileUtils.deleteRecursive(mapBaseDir, true);
    }

    boolean isClosed() {
        return closed;
    }

    void close() {
        closeStorage(false);
    }

    private void closeNow() {
        try {
            closeStorage(true);
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStorage(boolean now) {
        if (closed) {
            return;
        }
        if (!now) {
            save();
        }
        closed = true;
        synchronized (this) {
            chunkManager.close();
            if (cache != null)
                cache.clear();
        }
    }

    public void setUnsavedChanges(boolean b) {
        hasUnsavedChanges = b;
    }

    private boolean hasUnsavedChanges() {
        boolean b = hasUnsavedChanges;
        hasUnsavedChanges = false;
        return b;
    }

    synchronized void save() {
        if (closed) {
            return;
        }
        if (bTreeMap.isInMemory()) {
            return;
        }
        if (bTreeMap.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "This storage is read-only");
        }
        if (!hasUnsavedChanges()) {
            return;
        }
        try {
            executeSave();
            new ChunkCompactor(this, chunkManager).executeCompact();
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    public synchronized void executeSave() {
        if (bTreeMap.isInMemory()) {
            return;
        }
        DataBuffer chunkBody = DataBuffer.create();
        try {
            Chunk c = chunkManager.createChunk();
            c.fileStorage = getFileStorage(c.fileName);
            c.mapSize = bTreeMap.size();
            Page p = bTreeMap.getRootPage();
            p.writeUnsavedRecursive(c, chunkBody);
            c.rootPagePos = p.getPos();
            c.write(chunkBody, chunkManager.getRemovedPages());
            chunkManager.addChunk(c);
            chunkManager.setLastChunk(c);
        } catch (IllegalStateException e) {
            throw panic(e);
        } finally {
            chunkBody.close();
        }
    }

    public FileStorage getFileStorage(int chunkId) {
        String chunkFileName = mapBaseDir + File.separator + chunkManager.getChunkFileName(chunkId);
        return openFileStorage(chunkFileName);
    }

    private FileStorage getFileStorage(String chunkFileName) {
        chunkFileName = mapBaseDir + File.separator + chunkFileName;
        return openFileStorage(chunkFileName);
    }

    private FileStorage openFileStorage(String chunkFileName) {
        FileStorage fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, bTreeMap.getConfig());
        return fileStorage;
    }
}
