package com.glodon.base.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.HashMap;

import com.glodon.base.Constants;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.fs.FileUtils;

public class TempFileDeleter {

    private final ReferenceQueue<Object> queue = new ReferenceQueue<Object>();
    private final HashMap<PhantomReference<?>, String> refMap = new HashMap<>();

    private TempFileDeleter() {
    }

    public static TempFileDeleter getInstance() {
        return new TempFileDeleter();
    }

    public synchronized Reference<?> addFile(String fileName, Object file) {
        IOUtils.trace("TempFileDeleter.addFile", fileName, file);
        PhantomReference<?> ref = new PhantomReference<Object>(file, queue);
        refMap.put(ref, fileName);
        deleteUnused();
        return ref;
    }

    public synchronized void deleteFile(Reference<?> ref, String fileName) {
        if (ref != null) {
            String f2 = refMap.remove(ref);
            if (f2 != null) {
                if (Constants.CHECK) {
                    if (fileName != null && !f2.equals(fileName)) {
                        UnificationException.throwInternalError("f2:" + f2 + " f:" + fileName);
                    }
                }
                fileName = f2;
            }
        }
        if (fileName != null && FileUtils.exists(fileName)) {
            try {
                IOUtils.trace("TempFileDeleter.deleteFile", fileName, null);
                FileUtils.tryDelete(fileName);
            } catch (Exception e) {
                // TODO log such errors?
            }
        }
    }

    public void deleteAll() {
        for (String tempFile : new ArrayList<>(refMap.values())) {
            deleteFile(null, tempFile);
        }
        deleteUnused();
    }

    public void deleteUnused() {
        while (queue != null) {
            Reference<? extends Object> ref = queue.poll();
            if (ref == null) {
                break;
            }
            deleteFile(ref, null);
        }
    }

    public void stopAutoDelete(Reference<?> ref, String fileName) {
        IOUtils.trace("TempFileDeleter.stopAutoDelete", fileName, ref);
        if (ref != null) {
            String f2 = refMap.remove(ref);
            if (Constants.CHECK) {
                if (f2 == null || !f2.equals(fileName)) {
                    UnificationException.throwInternalError(
                            "f2:" + f2 + " " + (f2 == null ? "" : f2) + " f:" + fileName);
                }
            }
        }
        deleteUnused();
    }
}
