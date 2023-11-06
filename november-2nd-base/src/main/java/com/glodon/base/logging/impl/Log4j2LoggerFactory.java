package com.glodon.base.logging.impl;

import com.glodon.base.logging.LoggerFactory;

/**
 * Created by liujing on 2023/10/12.
 */
public class Log4j2LoggerFactory extends LoggerFactory {
    @Override
    public Log4j2Logger createLogger(String name) {
        return new Log4j2Logger(name);
    }
}
