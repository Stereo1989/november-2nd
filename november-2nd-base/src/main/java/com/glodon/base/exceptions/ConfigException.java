package com.glodon.base.exceptions;

public class ConfigException extends RuntimeException {

    public ConfigException(String msg) {
        super(msg);
    }

    public ConfigException(String msg, Throwable e) {
        super(msg, e);
    }
}
