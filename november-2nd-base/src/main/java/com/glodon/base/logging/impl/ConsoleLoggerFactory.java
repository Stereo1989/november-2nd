package com.glodon.base.logging.impl;

import com.glodon.base.logging.LoggerFactory;

/**
 * Created by liujing on 2023/10/12.
 */
public class ConsoleLoggerFactory extends LoggerFactory {
    @Override
    public ConsoleLogger createLogger(String name) {
        return new ConsoleLogger();
    }
}
