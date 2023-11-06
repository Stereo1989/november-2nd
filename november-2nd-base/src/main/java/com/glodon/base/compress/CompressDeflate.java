package com.glodon.base.compress;

import java.util.StringTokenizer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.glodon.base.exceptions.UnificationException;

public class CompressDeflate implements Compressor {

    private int level = Deflater.DEFAULT_COMPRESSION;
    private int strategy = Deflater.DEFAULT_STRATEGY;

    @Override
    public void setOptions(String options) {
        if (options == null) {
            return;
        }
        try {
            StringTokenizer tokenizer = new StringTokenizer(options);
            while (tokenizer.hasMoreElements()) {
                String option = tokenizer.nextToken();
                if ("level".equals(option) || "l".equals(option)) {
                    level = Integer.parseInt(tokenizer.nextToken());
                } else if ("strategy".equals(option) || "s".equals(option)) {
                    strategy = Integer.parseInt(tokenizer.nextToken());
                }
                Deflater deflater = new Deflater(level);
                deflater.setStrategy(strategy);
            }
        } catch (Exception e) {
            throw UnificationException.get("%s unsupported compression options.", options);
        }
    }

    @Override
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        Deflater deflater = new Deflater(level);
        deflater.setStrategy(strategy);
        deflater.setInput(in, 0, inLen);
        deflater.finish();
        int compressed = deflater.deflate(out, outPos, out.length - outPos);
        while (compressed == 0) {
            strategy = Deflater.DEFAULT_STRATEGY;
            level = Deflater.DEFAULT_COMPRESSION;
            return compress(in, inLen, out, outPos);
        }
        deflater.end();
        return outPos + compressed;
    }

    @Override
    public int getAlgorithm() {
        return DEFLATE;
    }

    @Override
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, inPos, inLen);
        decompresser.finished();
        try {
            int len = decompresser.inflate(out, outPos, outLen);
            if (len != outLen) {
                throw new DataFormatException(len + " " + outLen);
            }
        } catch (DataFormatException e) {
            throw UnificationException.get("compression error.", e);
        }
        decompresser.end();
    }

}
