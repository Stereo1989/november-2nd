package com.glodon.base.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Map;

import com.glodon.base.Constants;
import com.glodon.base.storage.DataHandler;
import com.glodon.base.storage.cache.FilePathCache;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.security.SecureFileStorage;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.TempFileDeleter;

/**
 * 借鉴h2文件工具
 * <p>
 * Created by liujing on 2023/10/12.
 */
public class FileStorage {

    public static final int HEADER_LENGTH = 3 * Constants.FILE_BLOCK_SIZE;

    private static final String HEADER = "-- H2 0.5/B --      ".substring(0,
            Constants.FILE_BLOCK_SIZE - 1) + "\n";

    protected String name;
    private DataHandler handler;
    private long filePos;
    private long fileLength;
    private Reference<?> autoDeleteReference;
    private boolean checkedWriting = true;
    private String mode;
    private TempFileDeleter tempFileDeleter;
    private FileLock lock;

    protected long readCount;
    protected long readBytes;
    protected long writeCount;
    protected long writeBytes;
    protected String fileName;
    protected boolean readOnly;
    protected long fileSize;
    protected FileChannel file;
    protected FileChannel encryptedFile;
    protected FileLock fileLock;

    @Override
    public String toString() {
        return fileName;
    }

    public FileStorage() {
    }

    public FileStorage(DataHandler handler, String name, String mode) {
        this.handler = handler;
        this.name = name;
        if (handler != null) {
            tempFileDeleter = handler.getTempFileDeleter();
        } else {
            tempFileDeleter = null;
        }
        try {
            boolean exists = FileUtils.exists(name);
            if (exists && !FileUtils.canWrite(name)) {
                mode = "r";
            } else {
                FileUtils.createDirectories(FileUtils.getParent(name));
            }
            file = FileUtils.open(name, mode);
            if (exists) {
                fileLength = file.size();
            }
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, "name: " + name + " mode: " + mode);
        }
        this.mode = mode;
    }

    public static FileStorage open(DataHandler handler, String name, String mode) {
        return open(handler, name, mode, null, null, 0);
    }

    public static FileStorage open(DataHandler handler, String name, String mode, String cipher,
                                   byte[] key) {
        return open(handler, name, mode, cipher, key, Constants.ENCRYPTION_KEY_HASH_ITERATIONS);
    }

    public static FileStorage open(DataHandler handler, String name, String mode, String cipher,
                                   byte[] key, int keyIterations) {
        FileStorage store;
        if (cipher == null) {
            store = new FileStorage(handler, name, mode);
        } else {
            store = new SecureFileStorage(handler, name, mode, cipher, key, keyIterations);
        }
        return store;
    }

    protected byte[] generateSalt() {
        return HEADER.getBytes();
    }

    protected void initKey(byte[] salt) {
    }

    public void setCheckedWriting(boolean value) {
        this.checkedWriting = value;
    }

    private void checkWritingAllowed() {
        if (handler != null && checkedWriting) {
            handler.checkWritingAllowed();
        }
    }

    private void checkPowerOff() {
        if (handler != null) {
            handler.checkPowerOff();
        }
    }

    public void init() {
        int len = Constants.FILE_BLOCK_SIZE;
        byte[] salt;
        byte[] magic = HEADER.getBytes();
        if (length() < HEADER_LENGTH) {
            // write unencrypted
            checkedWriting = false;
            writeDirect(magic, 0, len);
            salt = generateSalt();
            writeDirect(salt, 0, len);
            initKey(salt);
            // write (maybe) encrypted
            write(magic, 0, len);
            checkedWriting = true;
        } else {
            // read unencrypted
            seek(0);
            byte[] buff = new byte[len];
            readFullyDirect(buff, 0, len);
            if (!Arrays.equals(buff, magic)) {
                throw UnificationException.get("%s file version error.", name);
            }
            salt = new byte[len];
            readFullyDirect(salt, 0, len);
            initKey(salt);
            // read (maybe) encrypted
            readFully(buff, 0, Constants.FILE_BLOCK_SIZE);
            if (!Arrays.equals(buff, magic)) {
                throw UnificationException.get("%s file encryption error.", name);
            }
        }
    }

    // public void close() {
    // if (file != null) {
    // try {
    // trace("close", name, file);
    // file.close();
    // } catch (IOException e) {
    // throw DbException.convertIOException(e, name);
    // } finally {
    // file = null;
    // }
    // }
    // }

    public void closeSilently() {
        try {
            close();
        } catch (Exception e) {
        }
    }

    public void closeAndDeleteSilently() {
        if (file != null) {
            closeSilently();
            tempFileDeleter.deleteFile(autoDeleteReference, name);
            name = null;
        }
    }

    protected void readFullyDirect(byte[] b, int off, int len) {
        readFully(b, off, len);
    }

    public void readFully(byte[] b, int off, int len) {
        if (Constants.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            UnificationException.throwInternalError("unaligned read " + name + " len " + len);
        }
        checkPowerOff();
        try {
            FileUtils.readFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, name);
        }
        filePos += len;
    }

    public void seek(long pos) {
        if (Constants.CHECK && pos % Constants.FILE_BLOCK_SIZE != 0) {
            UnificationException.throwInternalError("unaligned seek " + name + " pos " + pos);
        }
        try {
            if (pos != filePos) {
                file.position(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, name);
        }
    }

    protected void writeDirect(byte[] b, int off, int len) {
        write(b, off, len);
    }

    public void write(byte[] b, int off, int len) {
        if (Constants.CHECK && (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            UnificationException.throwInternalError("unaligned write " + name + " len " + len);
        }
        checkWritingAllowed();
        checkPowerOff();
        try {
            FileUtils.writeFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            closeFileSilently();
            throw UnificationException.convertIOException(e, name);
        }
        filePos += len;
        fileLength = Math.max(filePos, fileLength);
    }

    public void setLength(long newLength) {
        if (Constants.CHECK && newLength % Constants.FILE_BLOCK_SIZE != 0) {
            UnificationException.throwInternalError("unaligned setLength " + name + " pos " + newLength);
        }
        checkPowerOff();
        checkWritingAllowed();
        try {
            if (newLength > fileLength) {
                long pos = filePos;
                file.position(newLength - 1);
                FileUtils.writeFully(file, ByteBuffer.wrap(new byte[1]));
                file.position(pos);
            } else {
                file.truncate(newLength);
            }
            fileLength = newLength;
        } catch (IOException e) {
            closeFileSilently();
            throw UnificationException.convertIOException(e, name);
        }
    }

    public long length() {
        try {
            long len = fileLength;
            if (Constants.CHECK2) {
                len = file.size();
                if (len != fileLength) {
                    UnificationException.throwInternalError(
                            "file " + name + " length " + len + " expected " + fileLength);
                }
            }
            if (Constants.CHECK2 && len % Constants.FILE_BLOCK_SIZE != 0) {
                long newLength = len + Constants.FILE_BLOCK_SIZE - (len % Constants.FILE_BLOCK_SIZE);
                file.truncate(newLength);
                fileLength = newLength;
                UnificationException.throwInternalError("unaligned file length " + name + " len " + len);
            }
            return len;
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, name);
        }
    }

    public long getFilePointer() {
        if (Constants.CHECK2) {
            try {
                if (file.position() != filePos) {
                    UnificationException.throwInternalError();
                }
            } catch (IOException e) {
                throw UnificationException.convertIOException(e, name);
            }
        }
        return filePos;
    }

    // public void sync() {
    // try {
    // file.force(true);
    // } catch (IOException e) {
    // closeFileSilently();
    // throw DbException.convertIOException(e, name);
    // }
    // }

    public void autoDelete() {
        if (autoDeleteReference == null) {
            autoDeleteReference = tempFileDeleter.addFile(name, this);
        }
    }

    public void stopAutoDelete() {
        tempFileDeleter.stopAutoDelete(autoDeleteReference, name);
        autoDeleteReference = null;
    }

    public void closeFile() throws IOException {
        file.close();
        file = null;
    }

    private void closeFileSilently() {
        try {
            file.close();
        } catch (IOException e) {
        }
    }

    public void openFile() throws IOException {
        if (file == null) {
            file = FileUtils.open(name, mode);
            file.position(filePos);
        }
    }

    private static void trace(String method, String fileName, Object o) {
        if (Constants.TRACE_IO) {
            System.out.println("FileStore." + method + " " + fileName + " " + o);
        }
    }

    public synchronized boolean tryLock() {
        try {
            lock = file.tryLock();
            return lock != null;
        } catch (Exception e) {
            return false;
        }
    }

    public synchronized void releaseLock() {
        if (file != null && lock != null) {
            try {
                lock.release();
            } catch (Exception e) {
            }
            lock = null;
        }
    }

    public ByteBuffer readFully(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        if (len > 0) {
            DataUtils.readFully(file, pos, dst);
            readCount++;
            readBytes += len;
        }
        return dst;
    }

    public void writeFully(long pos, ByteBuffer src) {
        int len = src.remaining();
        fileSize = Math.max(fileSize, pos + len);
        DataUtils.writeFully(file, pos, src);
        writeCount++;
        writeBytes += len;
    }

    public void open(String fileName, Map<String, ?> config) {
        if (file != null) {
            return;
        }
        char[] encryptionKey = (char[]) config.get("encryptionKey");
        boolean readOnly = config.containsKey("readOnly");

        if (fileName != null) {
            FilePath p = FilePath.get(fileName);
            if (p instanceof FilePathDisk && !fileName.startsWith(p.getScheme() + ":")) {
                FilePathNio.class.getName();
                fileName = "nio:" + fileName;
            }
        }
        this.fileName = fileName;
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw DataUtils.newIllegalArgumentException("Directory does not exist: {0}", parent);
        }
        if (f.exists() && !f.canWrite()) {
            readOnly = true;
        }
        this.readOnly = readOnly;
        try {
            file = f.open(readOnly ? "r" : "rw");
            if (encryptionKey != null) {
                byte[] key = FilePathEncrypt.getPasswordBytes(encryptionKey);
                encryptedFile = file;
                file = new FilePathEncrypt.FileEncrypt(fileName, key, file);
            }
            file = FilePathCache.wrap(file);
            try {
                if (readOnly) {
                    fileLock = file.tryLock(0, Long.MAX_VALUE, true);
                } else {
                    fileLock = file.tryLock();
                }
            } catch (OverlappingFileLockException e) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName, e);
            }
            if (fileLock == null) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_LOCKED,
                        "The file is locked: {0}", fileName);
            }
            fileSize = file.size();
        } catch (IOException e) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                    "Could not open file {0}", fileName, e);
        }
    }

    public void close() {
        try {
            trace("close", name, file);
            if (fileLock != null) {
                fileLock.release();
                fileLock = null;
            }
            if (file != null)
                file.close();
        } catch (Exception e) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Closing failed for file {0}", fileName, e);
        } finally {
            file = null;
        }
    }

    public void sync() {
        try {
            file.force(true);
        } catch (IOException e) {
            closeFileSilently();
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Could not sync file {0}", fileName, e);
        }
    }

    public long size() {
        return fileSize;
    }

    public void truncate(long size) {
        try {
            writeCount++;
            file.truncate(size);
            fileSize = Math.min(fileSize, size);
        } catch (IOException e) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Could not truncate file {0} to size {1}", fileName, size, e);
        }
    }

    public FileChannel getFile() {
        return file;
    }

    public FileChannel getEncryptedFile() {
        return encryptedFile;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public long getWriteBytes() {
        return writeBytes;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getReadBytes() {
        return readBytes;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public String getFileName() {
        return fileName;
    }

    public void delete() {
        FileUtils.delete(fileName);
    }

    public InputStream getInputStream() {
        checkPowerOff();
        int len = (int) fileSize;
        byte[] bytes = new byte[len];
        try {
            file.position(0);
            FileUtils.readFully(file, ByteBuffer.wrap(bytes, 0, len));
            file.position(filePos);
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, name);
        }
        return new ByteArrayInputStream(bytes);
    }
}
