package com.glodon.base.security;

import com.glodon.base.Constants;
import com.glodon.base.storage.DataHandler;
import com.glodon.base.util.MathUtils;
import com.glodon.base.fs.FileStorage;

public class SecureFileStorage extends FileStorage {

    private byte[] key;
    private final BlockCipher cipher;
    private final BlockCipher cipherForInitVector;
    private byte[] buffer = new byte[4];
    private long pos;
    private final byte[] bufferForInitVector;
    private final int keyIterations;

    public SecureFileStorage(DataHandler handler, String name, String mode, String cipher, byte[] key,
                             int keyIterations) {
        super(handler, name, mode);
        this.key = key;
        this.cipher = CipherFactory.getBlockCipher(cipher);
        this.cipherForInitVector = CipherFactory.getBlockCipher(cipher);
        this.keyIterations = keyIterations;
        bufferForInitVector = new byte[Constants.FILE_BLOCK_SIZE];
    }

    @Override
    protected byte[] generateSalt() {
        return MathUtils.secureRandomBytes(Constants.FILE_BLOCK_SIZE);
    }

    @Override
    protected void initKey(byte[] salt) {
        key = SHA256.getHashWithSalt(key, salt);
        for (int i = 0; i < keyIterations; i++) {
            key = SHA256.getHash(key, true);
        }
        cipher.setKey(key);
        key = SHA256.getHash(key, true);
        cipherForInitVector.setKey(key);
    }

    @Override
    protected void writeDirect(byte[] b, int off, int len) {
        super.write(b, off, len);
        pos += len;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (buffer.length < b.length) {
            buffer = new byte[len];
        }
        System.arraycopy(b, off, buffer, 0, len);
        xorInitVector(buffer, 0, len, pos);
        cipher.encrypt(buffer, 0, len);
        super.write(buffer, 0, len);
        pos += len;
    }

    @Override
    protected void readFullyDirect(byte[] b, int off, int len) {
        super.readFully(b, off, len);
        pos += len;
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        super.readFully(b, off, len);
        for (int i = 0; i < len; i++) {
            if (b[i] != 0) {
                cipher.decrypt(b, off, len);
                xorInitVector(b, off, len, pos);
                break;
            }
        }
        pos += len;
    }

    @Override
    public void seek(long x) {
        this.pos = x;
        super.seek(x);
    }

    private void xorInitVector(byte[] b, int off, int len, long p) {
        byte[] iv = bufferForInitVector;
        while (len > 0) {
            for (int i = 0; i < Constants.FILE_BLOCK_SIZE; i += 8) {
                long block = (p + i) >>> 3;
                iv[i] = (byte) (block >> 56);
                iv[i + 1] = (byte) (block >> 48);
                iv[i + 2] = (byte) (block >> 40);
                iv[i + 3] = (byte) (block >> 32);
                iv[i + 4] = (byte) (block >> 24);
                iv[i + 5] = (byte) (block >> 16);
                iv[i + 6] = (byte) (block >> 8);
                iv[i + 7] = (byte) block;
            }
            cipherForInitVector.encrypt(iv, 0, Constants.FILE_BLOCK_SIZE);
            for (int i = 0; i < Constants.FILE_BLOCK_SIZE; i++) {
                b[off + i] ^= iv[i];
            }
            p += Constants.FILE_BLOCK_SIZE;
            off += Constants.FILE_BLOCK_SIZE;
            len -= Constants.FILE_BLOCK_SIZE;
        }
    }

}
