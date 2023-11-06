package com.glodon.base.security;

import com.glodon.base.exceptions.UnificationException;

public class CipherFactory {

    private CipherFactory() {
    }

    public static BlockCipher getBlockCipher(String algorithm) {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        } else if ("FOG".equalsIgnoreCase(algorithm)) {
            return new Fog();
        }
        throw UnificationException.get("%s unsupported cipher.", algorithm);
    }
}
