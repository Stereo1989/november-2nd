package com.glodon.base.storage;

import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.fs.FileStorage;
import com.glodon.base.util.TempFileDeleter;

/**
 * Created by liujing on 2023/10/12.
 */
public interface DataHandler {

    FileStorage openFile(String name, String mode, boolean mustExist);

    TempFileDeleter getTempFileDeleter();

    void checkPowerOff() throws UnificationException;

    void checkWritingAllowed() throws UnificationException;
}
