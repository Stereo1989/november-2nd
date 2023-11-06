package com.glodon.base.compress;

public interface Compressor {

    int NO = 0;

    int LZF = 1;

    int DEFLATE = 2;

    int getAlgorithm();

    int compress(byte[] in, int inLen, byte[] out, int outPos);

    void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen);

    void setOptions(String options);
}
