package com.glodon.base;


import com.glodon.base.util.Utils;

import java.io.File;
import java.nio.charset.Charset;

public final class Constants {

    public static final String JAVA_SPECIFICATION_VERSION = Utils
            .getProperty("java.specification.version", "1.4");

    public static final String LINE_SEPARATOR = Utils.getProperty("line.separator", "\n");

    public static final String USER_HOME = Utils.getProperty("user.home", "");

    public static final String PROJECT_NAME = "november-2nd";

    public static final String PROJECT_NAME_PREFIX = PROJECT_NAME + ".";

    public static final String DEFAULT_BASE_DIR = USER_HOME + File.separator + PROJECT_NAME;

    public static final char NAME_SEPARATOR = '_';

    public static final int DEFAULT_PAGE_SIZE = 16 * 1024; // 16K;

    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;

    public static final int FILE_BLOCK_SIZE = 16;

    public static final int IO_BUFFER_SIZE = 4 * 1024;

    public static final int IO_BUFFER_SIZE_COMPRESS = 128 * 1024;

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");

    public static final String FILE_ENCODING = Utils.getProperty("file.encoding", "Cp1252");

    public static final String FILE_SEPARATOR = Utils.getProperty("file.separator", "/");

    public static final boolean CHECK = true;
    public static final boolean CHECK2 = false;

    public static final int MAX_FILE_RETRY = 16;

    public static final int OBJECT_CACHE_SIZE = 1024;

    public static final String SYNC_METHOD = "sync";

    public static final boolean TRACE_IO = false;

    private Constants() {
    }
}
