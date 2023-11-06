package com.glodon.base.compress;

public class CompressNo implements Compressor {

    @Override
    public int getAlgorithm() {
        return Compressor.NO;
    }

    @Override
    public void setOptions(String options) {
        // nothing to do
    }

    @Override
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        System.arraycopy(in, 0, out, outPos, inLen);
        return outPos + inLen;
    }

    @Override
    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        System.arraycopy(in, inPos, out, outPos, outLen);
    }

}
