package com.glodon.base.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.glodon.base.Constants;
import com.glodon.base.util.StringUtils;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.util.DataUtils;

public class CompressTool {

    private static final int MAX_BUFFER_SIZE = 3 * Constants.IO_BUFFER_SIZE_COMPRESS;
    private byte[] cachedBuffer;

    private CompressTool() {
    }

    private byte[] getBuffer(int min) {
        if (min > MAX_BUFFER_SIZE) {
            return DataUtils.newBytes(min);
        }
        if (cachedBuffer == null || cachedBuffer.length < min) {
            cachedBuffer = DataUtils.newBytes(min);
        }
        return cachedBuffer;
    }


    public static CompressTool getInstance() {
        return new CompressTool();
    }

    public byte[] compress(byte[] in, String algorithm) {
        int len = in.length;
        if (in.length < 5) {
            algorithm = "NO";
        }
        Compressor compress = getCompressor(algorithm);
        byte[] buff = getBuffer((len < 100 ? len + 100 : len) * 2);
        int newLen = compress(in, in.length, compress, buff);
        byte[] out = DataUtils.newBytes(newLen);
        System.arraycopy(buff, 0, out, 0, newLen);
        return out;
    }

    private static int compress(byte[] in, int len, Compressor compress, byte[] out) {
        int newLen = 0;
        out[0] = (byte) compress.getAlgorithm();
        int start = 1 + writeVariableInt(out, 1, len);
        newLen = compress.compress(in, len, out, start);
        if (newLen > len + start || newLen <= 0) {
            out[0] = Compressor.NO;
            System.arraycopy(in, 0, out, start, len);
            newLen = len + start;
        }
        return newLen;
    }

    public byte[] expand(byte[] in) {
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            byte[] buff = DataUtils.newBytes(len);
            compress.expand(in, start, in.length - start, buff, 0, len);
            return buff;
        } catch (Exception e) {
            throw UnificationException.get("compression error.", e);
        }
    }

    public static void expand(byte[] in, byte[] out, int outPos) {
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            compress.expand(in, start, in.length - start, out, outPos, len);
        } catch (Exception e) {
            throw UnificationException.get("compression error.", e);
        }
    }

    public static int readVariableInt(byte[] buff, int pos) {
        int x = buff[pos++] & 0xff;
        if (x < 0x80) {
            return x;
        }
        if (x < 0xc0) {
            return ((x & 0x3f) << 8) + (buff[pos] & 0xff);
        }
        if (x < 0xe0) {
            return ((x & 0x1f) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
        }
        if (x < 0xf0) {
            return ((x & 0xf) << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8)
                    + (buff[pos] & 0xff);
        }
        return ((buff[pos++] & 0xff) << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8)
                + (buff[pos] & 0xff);
    }

    public static int writeVariableInt(byte[] buff, int pos, int x) {
        if (x < 0) {
            buff[pos++] = (byte) 0xf0;
            buff[pos++] = (byte) (x >> 24);
            buff[pos++] = (byte) (x >> 16);
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 5;
        } else if (x < 0x80) {
            buff[pos] = (byte) x;
            return 1;
        } else if (x < 0x4000) {
            buff[pos++] = (byte) (0x80 | (x >> 8));
            buff[pos] = (byte) x;
            return 2;
        } else if (x < 0x200000) {
            buff[pos++] = (byte) (0xc0 | (x >> 16));
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 3;
        } else if (x < 0x10000000) {
            buff[pos++] = (byte) (0xe0 | (x >> 24));
            buff[pos++] = (byte) (x >> 16);
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 4;
        } else {
            buff[pos++] = (byte) 0xf0;
            buff[pos++] = (byte) (x >> 24);
            buff[pos++] = (byte) (x >> 16);
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 5;
        }
    }

    public static int getVariableIntLength(int x) {
        if (x < 0) {
            return 5;
        } else if (x < 0x80) {
            return 1;
        } else if (x < 0x4000) {
            return 2;
        } else if (x < 0x200000) {
            return 3;
        } else if (x < 0x10000000) {
            return 4;
        } else {
            return 5;
        }
    }

    private static Compressor getCompressor(String algorithm) {
        if (algorithm == null) {
            algorithm = "LZF";
        }
        int idx = algorithm.indexOf(' ');
        String options = null;
        if (idx > 0) {
            options = algorithm.substring(idx + 1);
            algorithm = algorithm.substring(0, idx);
        }
        int a = getCompressAlgorithm(algorithm);
        Compressor compress = getCompressor(a);
        compress.setOptions(options);
        return compress;
    }

    public static int getCompressAlgorithm(String algorithm) {
        algorithm = StringUtils.toUpperEnglish(algorithm);
        if ("NO".equals(algorithm)) {
            return Compressor.NO;
        } else if ("LZF".equals(algorithm)) {
            return Compressor.LZF;
        } else if ("DEFLATE".equals(algorithm)) {
            return Compressor.DEFLATE;
        } else {
            throw UnificationException.get("%s unsupported compression algorithm.", algorithm);
        }
    }

    private static Compressor getCompressor(int algorithm) {
        switch (algorithm) {
            case Compressor.NO:
                return new CompressNo();
            case Compressor.LZF:
                return new CompressLZF();
            case Compressor.DEFLATE:
                return new CompressDeflate();
            default:
                throw UnificationException.get("%s unsupported compression algorithm.", String.valueOf(algorithm));
        }
    }

    public static OutputStream wrapOutputStream(OutputStream out, String compressionAlgorithm,
                                                String entryName) {
        try {
            if ("GZIP".equals(compressionAlgorithm)) {
                out = new GZIPOutputStream(out);
            } else if ("ZIP".equals(compressionAlgorithm)) {
                ZipOutputStream z = new ZipOutputStream(out);
                z.putNextEntry(new ZipEntry(entryName));
                out = z;
            } else if ("DEFLATE".equals(compressionAlgorithm)) {
                out = new DeflaterOutputStream(out);
            } else if ("LZF".equals(compressionAlgorithm)) {
                out = new LZFOutputStream(out);
            } else if (compressionAlgorithm != null) {
                throw UnificationException.get("%s unsupported compression algorithm.", compressionAlgorithm);
            }
            return out;
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, null);
        }
    }

    public static InputStream wrapInputStream(InputStream in, String compressionAlgorithm,
                                              String entryName) {
        try {
            if ("GZIP".equals(compressionAlgorithm)) {
                in = new GZIPInputStream(in);
            } else if ("ZIP".equals(compressionAlgorithm)) {
                ZipInputStream z = new ZipInputStream(in);
                while (true) {
                    ZipEntry entry = z.getNextEntry();
                    if (entry == null) {
                        return null;
                    }
                    if (entryName.equals(entry.getName())) {
                        break;
                    }
                }
                in = z;
            } else if ("DEFLATE".equals(compressionAlgorithm)) {
                in = new InflaterInputStream(in);
            } else if ("LZF".equals(compressionAlgorithm)) {
                in = new LZFInputStream(in);
            } else if (compressionAlgorithm != null) {
                throw UnificationException.get("%s unsupported compression algorithm.", compressionAlgorithm);
            }
            return in;
        } catch (IOException e) {
            throw UnificationException.convertIOException(e, null);
        }
    }
}
