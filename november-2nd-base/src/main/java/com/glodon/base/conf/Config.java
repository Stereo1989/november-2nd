package com.glodon.base.conf;

import com.glodon.base.Constants;
import com.glodon.base.compress.Compressor;
import com.glodon.base.storage.page.PageOperationHandlerFactory;
import com.glodon.base.util.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by liujing on 2023/10/15.
 */
public class Config extends HashMap<String, Object> {

    public static final Config DEFAULT = new Config();

    public static final String EMBEDDED = "embedded";
    public static final String STORAGE_PATH = "storage.path";
    public static final String STORAGE_COMPRESS = "storage.compress";
    public static final String STORAGE_IN_MEMORY = "storage.in.memory";
    public static final String STORAGE_READ_ONLY = "storage.read.only";
    public static final String STORAGE_CACHE_SIZE = "storage.cache.size";
    public static final String STORAGE_PAGE_SPLIT_SIZE = "storage.page.split.size";
    public static final String STORAGE_MIN_FILL_RATE = "storage.min.fill.rate";

    public static final String STORAGE_PAGE_MODE = "storage.page.mode";
    public static final String STORAGE_BACKGROUND_EXCEPTION_HANDLER = "storage.background.exception.handler";
    public static final String STORAGE_PAGE_OPERATION_HANDLER_SIZE = "storage.page.operation.handler.size";
    public static final String STORAGE_PAGE_OPERATION_HANDLER_FACTORY_TYPE = "storage.page.operation.handler.factory.type";
    public static final String STORAGE_PAGE_OPERATION_HANDLER_LOOP_INTERVAL = "storage.page.operation.handler.loop.interval";
    public static final String STORAGE_PAGE_OPERATION_HANDLER_FACTORY = "storage.page.operation.handler.factory";

    public final boolean isEmbedded() {
        return getBoolean(EMBEDDED, true);
    }

    public final int getStorageCacheSize() {
        return getIntValue(STORAGE_CACHE_SIZE, 16 * 1024 * 1024);
    }

    public final int getStoragePageSplitSize() {
        return getIntValue(STORAGE_PAGE_SPLIT_SIZE, 8 * 1024);
    }

    public final int getCompressionLevel() {
        Object value = getVal(STORAGE_COMPRESS);
        if (value == null)
            return Compressor.NO;
        else {
            String str = value.toString().trim().toUpperCase();
            if (str.equals("NO")) {
                return Compressor.NO;
            } else if (str.equals("LZF")) {
                return Compressor.LZF;
            } else if (str.equals("DEFLATE")) {

                return Compressor.DEFLATE;
            } else {
                return Integer.valueOf(str);
            }
        }
    }

    public final int getStorageMinFillRate() {
        int minFillRate = getIntValue(STORAGE_MIN_FILL_RATE, 30);
        if (minFillRate > 50) {
            minFillRate = 50;
        }
        return minFillRate;
    }

    public final String getPageStorageMode() {
        return getStr(STORAGE_PAGE_MODE);
    }

    public final Thread.UncaughtExceptionHandler getStorageBackgroundExceptionHandler() {
        return (Thread.UncaughtExceptionHandler) getVal(STORAGE_BACKGROUND_EXCEPTION_HANDLER);
    }

    public final String getStoragePageOperationHandlerFactoryType() {
        return getStr(STORAGE_PAGE_OPERATION_HANDLER_FACTORY_TYPE, "LoadBalance");
    }

    public final int getStoragePageOperationHandlerSize() {
        return getInt(STORAGE_PAGE_OPERATION_HANDLER_SIZE, 1);
    }

    public final long getStoragePageOperationHandlerLoopInterval() {
        return getLong(STORAGE_PAGE_OPERATION_HANDLER_LOOP_INTERVAL, 100l);
    }

    public final PageOperationHandlerFactory getPageOperationHandlerFactory() {
        return (PageOperationHandlerFactory) getVal(STORAGE_PAGE_OPERATION_HANDLER_FACTORY);
    }

    public final boolean isInMemory() {
        return getBoolean(STORAGE_IN_MEMORY, false);
    }

    public final String getStoragePath() {
        return getStr(Config.STORAGE_PATH, Constants.DEFAULT_BASE_DIR);
    }

    public final boolean isReadOnly() {
        return getBoolean(STORAGE_READ_ONLY, false);
    }

    public Config setVal(String key, Object val) {
        this.put(key, val);
        return this;
    }

    public Object getVal(String key) {
        return get(key);
    }

    public String getStr(String key) {
        Object obj = getVal(key);
        if (obj != null) {
            return obj.toString();
        }
        return null;
    }

    public String getStr(String key, String def) {
        String val = getStr(key);
        if (val == null) {
            return def;
        }
        return val;
    }


    public Long getLong(String key) {
        Object val = getVal(key);
        if (val != null) {
            return Long.parseLong(val.toString());
        }
        return null;
    }

    public Long getLong(String key, Long def) {
        Long val = getLong(key);
        if (val == null) {
            return def;
        }
        return val;
    }


    public Integer getInt(String key) {
        Object val = getVal(key);
        if (val != null) {
            return Integer.parseInt(val.toString());
        }
        return null;
    }

    public Integer getInt(String key, Integer def) {
        Integer val = getInt(key);
        if (val == null) {
            return def;
        }
        return val;
    }

    public Boolean getBoolean(String key) {
        Object val = getVal(key);
        if (val != null) {
            return Boolean.parseBoolean(val.toString());
        }
        return null;
    }

    public Boolean getBoolean(String key, Boolean def) {
        Boolean b = getBoolean(key);
        if (b == null) {
            return def;
        }
        return b;
    }

    public Double getDouble(String key) {
        Object val = getVal(key);
        if (val != null) {
            return Double.parseDouble(val.toString());
        }
        return null;
    }

    public Double getDouble(String key, Double def) {
        Double d = getDouble(key);
        if (d == null) {
            return def;
        }
        return d;
    }

    @Override
    public Object get(Object key) {
        return super.get(toUpper(key));
    }

    @Override
    public Object put(String key, Object value) {
        return super.put(toUpper(key), value);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        if (m != null) {
            for (Map.Entry<? extends String, ?> e : m.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(toUpper(key));
    }

    @Override
    public Object remove(Object key) {
        return super.remove(toUpper(key));
    }

    private static String toUpper(Object key) {
        return key == null ? null : StringUtils.toUpperEnglish(key.toString());
    }

    public void removeAll(Collection<?> c) {
        for (Object e : c) {
            remove(e);
        }
    }

    public void putAll(Properties prop) {
        for (Entry<Object, Object> e : prop.entrySet()) {
            put(e.getKey().toString(), e.getValue().toString());
        }
    }

    private int getIntValue(String key, int defaultValue) {
        Object value = getVal(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value != null) {
            String str = value.toString().trim().toLowerCase();
            if (str.endsWith("k")) {
                str = str.substring(0, str.length() - 1).trim();
                return Integer.parseInt(str) * 1024;
            } else if (str.endsWith("m")) {
                str = str.substring(0, str.length() - 1).trim();
                return Integer.parseInt(str) * 1024 * 1024;
            } else {
                return Integer.parseInt(str);
            }
        } else {
            return defaultValue;
        }
    }
}
