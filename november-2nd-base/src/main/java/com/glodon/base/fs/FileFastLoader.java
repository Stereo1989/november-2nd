package com.glodon.base.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by liujing on 2023/10/16.
 */
public abstract class FileFastLoader implements FileLoader {

    protected abstract String getPath();

    protected abstract void completeLoading(byte[] bytes);

    public void loading() {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(getPath());
            ByteArrayOutputStream os = new ByteArrayOutputStream(fis.available());
            byte[] buf = new byte[1 * 1024 * 1024];
            int len;
            while ((len = fis.read(buf)) > 0) {
                os.write(buf, 0, len);
            }
            completeLoading(os.toByteArray());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
//        try {
//            fis = new FileInputStream(getPath());
//            ByteArrayOutputStream os = new ByteArrayOutputStream(fis.available());
//            WritableByteChannel targetChannel = Channels.newChannel(os);
//            FileChannel inputChannel = fis.getChannel();
//            inputChannel.transferTo(0, inputChannel.size(), targetChannel);
//            inputChannel.close();
//            targetChannel.close();
//            completeLoading(os.toByteArray());
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (fis != null) {
//                try {
//                    fis.close();
//                } catch (IOException e) {
//                }
//            }
//        }
    }

//    public static void main(String[] args) {
//        for (int i = 0; i < 10; i++) {
//            FileFastLoader fileFastLoader = new FileFastLoader() {
//                @Override
//                protected String getPath() {
//                    return "/Users/liujing/november-2nd/my_table/CLUSTERED_INDEX/c_2_2.db";
//                }
//
//                @Override
//                protected void completeLoading(byte[] bytes) {
//                    System.out.println("FileFastLoader.completeLoading " + bytes.length);
//                }
//            };
//            long s = System.currentTimeMillis();
//            fileFastLoader.loading();
//            System.out.println("FileFastLoader.main " + (System.currentTimeMillis() - s));
//        }
//    }
}
