package com.glodon.base.storage;

import java.util.Map;
import java.util.Set;

import com.glodon.base.Constants;
import com.glodon.base.storage.type.ObjectDataType;
import com.glodon.base.storage.type.StorageDataType;

/**
 * Created by liujing on 2023/10/12.
 */
public interface Storage {

    String SUFFIX_AO_FILE = ".db";
    int SUFFIX_AO_FILE_LENGTH = SUFFIX_AO_FILE.length();
    String TEMP_NAME_PREFIX = Constants.NAME_SEPARATOR + "temp" + Constants.NAME_SEPARATOR;

    default <K, V> StorageMap<K, V> openMap(String name, Map<String, String> parameters) {
        return openMap(name, new ObjectDataType(), new ObjectDataType(), parameters);
    }

    <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType);

    <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType, Map<String, String> parameters);

    void closeMap(String name);

    boolean hasMap(String name);

    StorageMap<?, ?> getMap(String name);

    Set<String> getMapNames();

    default String getTempMapName() {
        int i = 0;
        String name;
        while (true) {
            name = TEMP_NAME_PREFIX + i++;
            if (!hasMap(name)) {
                return name;
            }
        }
    }

    String getStoragePath();

    boolean isInMemory();

    void save();

    void drop();

    void close();

    void closeNow();

    boolean isClosed();

    void registerEventListener(StorageEventListener listener);

    void unregisterEventListener(StorageEventListener listener);
}
