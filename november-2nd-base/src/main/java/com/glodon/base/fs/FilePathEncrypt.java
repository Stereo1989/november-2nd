package com.glodon.base.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;

import com.glodon.base.Constants;
import com.glodon.base.security.AES;
import com.glodon.base.security.BlockCipher;
import com.glodon.base.security.SHA256;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.MathUtils;

/**
 * Created by liujing on 2023/10/12.
 */
public class FilePathEncrypt extends FilePathWrapper {

    private static final String SCHEME = "encrypt";

    public static void register() {
        register(new FilePathEncrypt());
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        String[] parsed = parse(name);
        FileChannel file = FileUtils.open(parsed[1], mode);
        byte[] passwordBytes = parsed[0].getBytes(Constants.UTF8);
        return new FileEncrypt(name, passwordBytes, file);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    protected String getPrefix() {
        String[] parsed = parse(name);
        return getScheme() + ":" + parsed[0] + ":";
    }

    @Override
    public FilePath unwrap(String fileName) {
        return get(parse(fileName)[1]);
    }

    @Override
    public long size() {
        long size = getBase().size() - FileEncrypt.HEADER_LENGTH;
        size = Math.max(0, size);
        if ((size & FileEncrypt.BLOCK_SIZE_MASK) != 0) {
            size -= FileEncrypt.BLOCK_SIZE;
        }
        return size;
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return new FileChannelOutputStream(open("rw"), append);
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return new FileChannelInputStream(open("r"), true);
    }

    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            throw new IllegalArgumentException(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        int idx = fileName.indexOf(':');
        String password;
        if (idx < 0) {
            throw new IllegalArgumentException(
                    fileName + " doesn't contain encryption algorithm and password");
        }
        password = fileName.substring(0, idx);
        fileName = fileName.substring(idx + 1);
        return new String[]{password, fileName};
    }

    public static byte[] getPasswordBytes(char[] passwordChars) {
        // using UTF-16
        int len = passwordChars.length;
        byte[] password = new byte[len * 2];
        for (int i = 0; i < len; i++) {
            char c = passwordChars[i];
            password[i + i] = (byte) (c >>> 8);
            password[i + i + 1] = (byte) c;
        }
        return password;
    }

    public static class FileEncrypt extends FileBase {

        static final int BLOCK_SIZE = 4096;

        static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;

        static final int HEADER_LENGTH = BLOCK_SIZE;

        private static final byte[] HEADER = "H2encrypt\n".getBytes();
        private static final int SALT_POS = HEADER.length;

        private static final int SALT_LENGTH = 8;

        private static final int HASH_ITERATIONS = 10;

        private final FileChannel base;

        private long pos;

        private long size;

        private final String name;

        private XTS xts;

        private byte[] encryptionKey;

        public FileEncrypt(String name, byte[] encryptionKey, FileChannel base) {
            this.name = name;
            this.base = base;
            this.encryptionKey = encryptionKey;
        }

        private void init() throws IOException {
            if (xts != null) {
                return;
            }
            this.size = base.size() - HEADER_LENGTH;
            boolean newFile = size < 0;
            byte[] salt;
            if (newFile) {
                byte[] header = Arrays.copyOf(HEADER, BLOCK_SIZE);
                salt = MathUtils.secureRandomBytes(SALT_LENGTH);
                System.arraycopy(salt, 0, header, SALT_POS, salt.length);
                DataUtils.writeFully(base, 0, ByteBuffer.wrap(header));
                size = 0;
            } else {
                salt = new byte[SALT_LENGTH];
                DataUtils.readFully(base, SALT_POS, ByteBuffer.wrap(salt));
                if ((size & BLOCK_SIZE_MASK) != 0) {
                    size -= BLOCK_SIZE;
                }
            }
            AES cipher = new AES();
            cipher.setKey(SHA256.getPBKDF2(encryptionKey, salt, HASH_ITERATIONS, 16));
            encryptionKey = null;
            xts = new XTS(cipher);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            base.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            this.pos = newPosition;
            return this;
        }

        @Override
        public long position() throws IOException {
            return pos;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int len = read(dst, pos);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            int len = dst.remaining();
            if (len == 0) {
                return 0;
            }
            init();
            len = (int) Math.min(len, size - position);
            if (position >= size) {
                return -1;
            } else if (position < 0) {
                throw new IllegalArgumentException("pos: " + position);
            }
            if ((position & BLOCK_SIZE_MASK) != 0 || (len & BLOCK_SIZE_MASK) != 0) {
                long p = position / BLOCK_SIZE * BLOCK_SIZE;
                int offset = (int) (position - p);
                int l = (len + offset + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                ByteBuffer temp = ByteBuffer.allocate(l);
                readInternal(temp, p, l);
                temp.flip();
                temp.limit(offset + len);
                temp.position(offset);
                dst.put(temp);
                return len;
            }
            readInternal(dst, position, len);
            return len;
        }

        private void readInternal(ByteBuffer dst, long position, int len) throws IOException {
            int x = dst.position();
            readFully(base, position + HEADER_LENGTH, dst);
            long block = position / BLOCK_SIZE;
            while (len > 0) {
                xts.decrypt(block++, BLOCK_SIZE, dst.array(), dst.arrayOffset() + x);
                x += BLOCK_SIZE;
                len -= BLOCK_SIZE;
            }
        }

        private static void readFully(FileChannel file, long pos, ByteBuffer dst) throws IOException {
            do {
                int len = file.read(dst, pos);
                if (len < 0) {
                    throw new EOFException();
                }
                pos += len;
            } while (dst.remaining() > 0);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            init();
            int len = src.remaining();
            if ((position & BLOCK_SIZE_MASK) != 0 || (len & BLOCK_SIZE_MASK) != 0) {
                long p = position / BLOCK_SIZE * BLOCK_SIZE;
                int offset = (int) (position - p);
                int l = (len + offset + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                ByteBuffer temp = ByteBuffer.allocate(l);
                int available = (int) (size - p + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                int readLen = Math.min(l, available);
                if (readLen > 0) {
                    readInternal(temp, p, readLen);
                    temp.rewind();
                }
                temp.limit(offset + len);
                temp.position(offset);
                temp.put(src);
                temp.limit(l);
                temp.rewind();
                writeInternal(temp, p, l);
                long p2 = position + len;
                size = Math.max(size, p2);
                int plus = (int) (size & BLOCK_SIZE_MASK);
                if (plus > 0) {
                    temp = ByteBuffer.allocate(plus);
                    DataUtils.writeFully(base, p + HEADER_LENGTH + l, temp);
                }
                return len;
            }
            writeInternal(src, position, len);
            long p2 = position + len;
            size = Math.max(size, p2);
            return len;
        }

        private void writeInternal(ByteBuffer src, long position, int len) throws IOException {
            ByteBuffer crypt = ByteBuffer.allocate(len);
            crypt.put(src);
            crypt.flip();
            long block = position / BLOCK_SIZE;
            int x = 0, l = len;
            while (l > 0) {
                xts.encrypt(block++, BLOCK_SIZE, crypt.array(), crypt.arrayOffset() + x);
                x += BLOCK_SIZE;
                l -= BLOCK_SIZE;
            }
            writeFully(base, position + HEADER_LENGTH, crypt);
        }

        private static void writeFully(FileChannel file, long pos, ByteBuffer src) throws IOException {
            int off = 0;
            do {
                int len = file.write(src, pos + off);
                off += len;
            } while (src.remaining() > 0);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int len = write(src, pos);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public long size() throws IOException {
            init();
            return size;
        }

        @Override
        public FileChannel truncate(long newSize) throws IOException {
            init();
            if (newSize > size) {
                return this;
            }
            if (newSize < 0) {
                throw new IllegalArgumentException("newSize: " + newSize);
            }
            int offset = (int) (newSize & BLOCK_SIZE_MASK);
            if (offset > 0) {
                base.truncate(newSize + HEADER_LENGTH + BLOCK_SIZE);
            } else {
                base.truncate(newSize + HEADER_LENGTH);
            }
            this.size = newSize;
            pos = Math.min(pos, size);
            return this;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return base.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return name;
        }

    }

    static class XTS {

        private static final int GF_128_FEEDBACK = 0x87;

        private static final int CIPHER_BLOCK_SIZE = 16;

        private final BlockCipher cipher;

        XTS(BlockCipher cipher) {
            this.cipher = cipher;
        }

        void encrypt(long id, int len, byte[] data, int offset) {
            byte[] tweak = initTweak(id);
            int i = 0;
            for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
                if (i > 0) {
                    updateTweak(tweak);
                }
                xorTweak(data, i + offset, tweak);
                cipher.encrypt(data, i + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i + offset, tweak);
            }
            if (i < len) {
                updateTweak(tweak);
                swap(data, i + offset, i - CIPHER_BLOCK_SIZE + offset, len - i);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
                cipher.encrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweak);
            }
        }

        void decrypt(long id, int len, byte[] data, int offset) {
            byte[] tweak = initTweak(id), tweakEnd = tweak;
            int i = 0;
            for (; i + CIPHER_BLOCK_SIZE <= len; i += CIPHER_BLOCK_SIZE) {
                if (i > 0) {
                    updateTweak(tweak);
                    if (i + CIPHER_BLOCK_SIZE + CIPHER_BLOCK_SIZE > len && i + CIPHER_BLOCK_SIZE < len) {
                        tweakEnd = Arrays.copyOf(tweak, CIPHER_BLOCK_SIZE);
                        updateTweak(tweak);
                    }
                }
                xorTweak(data, i + offset, tweak);
                cipher.decrypt(data, i + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i + offset, tweak);
            }
            if (i < len) {
                swap(data, i, i - CIPHER_BLOCK_SIZE + offset, len - i + offset);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweakEnd);
                cipher.decrypt(data, i - CIPHER_BLOCK_SIZE + offset, CIPHER_BLOCK_SIZE);
                xorTweak(data, i - CIPHER_BLOCK_SIZE + offset, tweakEnd);
            }
        }

        private byte[] initTweak(long id) {
            byte[] tweak = new byte[CIPHER_BLOCK_SIZE];
            for (int j = 0; j < CIPHER_BLOCK_SIZE; j++, id >>>= 8) {
                tweak[j] = (byte) (id & 0xff);
            }
            cipher.encrypt(tweak, 0, CIPHER_BLOCK_SIZE);
            return tweak;
        }

        private static void xorTweak(byte[] data, int pos, byte[] tweak) {
            for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
                data[pos + i] ^= tweak[i];
            }
        }

        private static void updateTweak(byte[] tweak) {
            byte ci = 0, co = 0;
            for (int i = 0; i < CIPHER_BLOCK_SIZE; i++) {
                co = (byte) ((tweak[i] >> 7) & 1);
                tweak[i] = (byte) (((tweak[i] << 1) + ci) & 255);
                ci = co;
            }
            if (co != 0) {
                tweak[0] ^= GF_128_FEEDBACK;
            }
        }

        private static void swap(byte[] data, int source, int target, int len) {
            for (int i = 0; i < len; i++) {
                byte temp = data[source + i];
                data[source + i] = data[target + i];
                data[target + i] = temp;
            }
        }
    }
}
