package com.glodon.base.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.glodon.base.util.MathUtils;
import com.glodon.base.util.Utils;

/**
 * Created by liujing on 2023/10/12.
 */
public abstract class FilePath {

    private static FilePath defaultProvider;

    private static Map<String, FilePath> providers;

    private static String tempRandom;
    private static long tempSequence;

    protected String name;

    public static FilePath get(String path) {
        path = path.replace('\\', '/');
        int index = path.indexOf(':');
        registerDefaultProviders();
        if (index < 2) {
            return defaultProvider.getPath(path);
        }
        String scheme = path.substring(0, index);
        FilePath p = providers.get(scheme);
        if (p == null) {
            p = defaultProvider;
        }
        return p.getPath(path);
    }

    private static void registerDefaultProviders() {
        if (providers == null || defaultProvider == null) {
            String packageName = FilePath.class.getPackage().getName() + ".";
            Map<String, FilePath> map = Collections.synchronizedMap(new HashMap<>());
            for (String c : new String[]{"FilePathDisk", "FilePathNio", "FilePathZip"}) {
                try {
                    FilePath p = Utils.newInstance(packageName + c);
                    map.put(p.getScheme(), p);
                    if (defaultProvider == null) {
                        defaultProvider = p;
                    }
                } catch (Exception e) {
                }
            }
            providers = map;
        }
    }

    public static void register(FilePath provider) {
        registerDefaultProviders();
        providers.put(provider.getScheme(), provider);
    }

    public static void unregister(FilePath provider) {
        registerDefaultProviders();
        providers.remove(provider.getScheme());
    }

    public abstract long size();

    public abstract void moveTo(FilePath newName, boolean atomicReplace);

    public abstract boolean createFile();

    public abstract boolean exists();

    public abstract void delete();

    public abstract List<FilePath> newDirectoryStream();

    public abstract FilePath toRealPath();

    public abstract FilePath getParent();

    public abstract boolean isDirectory();

    public abstract boolean isAbsolute();

    public abstract long lastModified();

    public abstract boolean canWrite();

    public abstract void createDirectory();

    public String getName() {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    public abstract OutputStream newOutputStream(boolean append) throws IOException;

    public abstract FileChannel open(String mode) throws IOException;

    public abstract InputStream newInputStream() throws IOException;

    public abstract boolean setReadOnly();

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        while (true) {
            FilePath p = getPath(name + getNextTempFileNamePart(false) + suffix);
            if (p.exists() || !p.createFile()) {
                getNextTempFileNamePart(true);
                continue;
            }
            p.open("rw").close();
            return p;
        }
    }

    protected static synchronized String getNextTempFileNamePart(boolean newRandom) {
        if (newRandom || tempRandom == null) {
            tempRandom = MathUtils.randomInt(Integer.MAX_VALUE) + ".";
        }
        return tempRandom + tempSequence++;
    }

    @Override
    public String toString() {
        return name;
    }

    public abstract String getScheme();

    public abstract FilePath getPath(String path);

    public FilePath unwrap() {
        return this;
    }
}
