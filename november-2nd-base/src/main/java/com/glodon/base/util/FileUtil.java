package com.glodon.base.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-26 15:24
 **/
public class FileUtil {
    public static byte[] readBytes(File file) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File must not be null");
        }

        try (InputStream inputStream = new FileInputStream(file)) {
            long length = file.length();

            if (length > Integer.MAX_VALUE) {
                throw new IOException("File is too large");
            }

            byte[] bytes = new byte[(int) length];
            int offset = 0;
            int bytesRead;

            while (offset < bytes.length && (bytesRead = inputStream.read(bytes, offset, bytes.length - offset)) != -1) {
                offset += bytesRead;
            }

            if (offset < bytes.length) {
                throw new IOException("Could not completely read file " + file.getName());
            }

            return bytes;
        }
    }
}
