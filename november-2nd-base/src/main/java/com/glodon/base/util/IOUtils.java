package com.glodon.base.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import com.glodon.base.Constants;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.fs.FileUtils;

public class IOUtils {

    private IOUtils() {
    }

    public static void closeSilently(Closeable out) {
        if (out != null) {
            try {
                trace("closeSilently", null, out);
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static void skipFully(InputStream in, long skip) throws IOException {
        try {
            while (skip > 0) {
                long skipped = in.skip(skip);
                if (skipped <= 0) {
                    throw new EOFException();
                }
                skip -= skipped;
            }
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        }
    }

    public static void skipFully(Reader reader, long skip) throws IOException {
        try {
            while (skip > 0) {
                long skipped = reader.skip(skip);
                if (skipped <= 0) {
                    throw new EOFException();
                }
                skip -= skipped;
            }
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        }
    }

    public static long copyAndClose(InputStream in, OutputStream out) throws IOException {
        try {
            long len = copyAndCloseInput(in, out);
            out.close();
            return len;
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        } finally {
            closeSilently(out);
        }
    }

    public static long copyAndCloseInput(InputStream in, OutputStream out) throws IOException {
        try {
            return copy(in, out);
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        } finally {
            closeSilently(in);
        }
    }

    public static long copy(InputStream in, OutputStream out) throws IOException {
        return copy(in, out, Long.MAX_VALUE);
    }

    public static long copy(InputStream in, OutputStream out, long length) throws IOException {
        try {
            long copied = 0;
            int len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            byte[] buffer = new byte[len];
            while (length > 0) {
                len = in.read(buffer, 0, len);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.write(buffer, 0, len);
                }
                copied += len;
                length -= len;
                len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            }
            return copied;
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        }
    }

    public static long copyAndCloseInput(Reader in, Writer out, long length) throws IOException {
        try {
            long copied = 0;
            int len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            char[] buffer = new char[len];
            while (length > 0) {
                len = in.read(buffer, 0, len);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.write(buffer, 0, len);
                }
                length -= len;
                len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
                copied += len;
            }
            return copied;
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        } finally {
            in.close();
        }
    }

    public static void closeSilently(InputStream in) {
        if (in != null) {
            try {
                trace("closeSilently", null, in);
                in.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static void closeSilently(Reader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static void closeSilently(Writer writer) {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static byte[] readBytesAndClose(InputStream in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(Constants.IO_BUFFER_SIZE, length);
            ByteArrayOutputStream out = new ByteArrayOutputStream(block);
            copy(in, out, length);
            return out.toByteArray();
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        } finally {
            in.close();
        }
    }

    public static String readStringAndClose(Reader in, int length) throws IOException {
        try {
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            int block = Math.min(Constants.IO_BUFFER_SIZE, length);
            StringWriter out = new StringWriter(block);
            copyAndCloseInput(in, out, length);
            return out.toString();
        } finally {
            in.close();
        }
    }

    public static int readFully(InputStream in, byte[] buffer) throws IOException {
        try {
            int result = 0, len = buffer.length;
            while (len > 0) {
                int l = in.read(buffer, result, len);
                if (l < 0) {
                    break;
                }
                result += l;
                len -= l;
            }
            return result;
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        }
    }

    public static int readFully(Reader in, char[] buffer) throws IOException {
        try {
            int off = 0, len = buffer.length;
            if (len == 0) {
                return 0;
            }
            while (true) {
                int l = len - off;
                if (l <= 0) {
                    break;
                }
                l = in.read(buffer, off, l);
                if (l < 0) {
                    break;
                }
                off += l;
            }
            return off <= 0 ? -1 : off;
        } catch (Exception e) {
            throw UnificationException.convertToIOException(e);
        }
    }

    public static Reader getBufferedReader(InputStream in) {
        return in == null ? null : new BufferedReader(new InputStreamReader(in, Constants.UTF8));
    }

    public static Reader getReader(InputStream in) {
        // InputStreamReader may read some more bytes
        return in == null ? null : new BufferedReader(new InputStreamReader(in, Constants.UTF8));
    }

    public static Writer getBufferedWriter(OutputStream out) {
        return out == null ? null : new BufferedWriter(new OutputStreamWriter(out, Constants.UTF8));
    }

    public static Reader getAsciiReader(InputStream in) {
        try {
            return in == null ? null : new InputStreamReader(in, "US-ASCII");
        } catch (Exception e) {
            // UnsupportedEncodingException
            throw UnificationException.convert(e);
        }
    }

    public static void trace(String method, String fileName, Object o) {
        if (Constants.TRACE_IO) {
            System.out.println("IOUtils." + method + " " + fileName + " " + o);
        }
    }

    public static InputStream getInputStreamFromString(String s) {
        if (s == null) {
            return null;
        }
        return new ByteArrayInputStream(s.getBytes(Constants.UTF8));
    }

    public static void copyFiles(String original, String copy) throws IOException {
        InputStream in = FileUtils.newInputStream(original);
        OutputStream out = FileUtils.newOutputStream(copy, false);
        copyAndClose(in, out);
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }
}
