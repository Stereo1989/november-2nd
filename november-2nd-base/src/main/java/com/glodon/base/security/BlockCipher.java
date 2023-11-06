package com.glodon.base.security;

public interface BlockCipher {

    int ALIGN = 16;

    void setKey(byte[] key);

    int getKeyLength();

    void encrypt(byte[] bytes, int off, int len);

    void decrypt(byte[] bytes, int off, int len);
}
