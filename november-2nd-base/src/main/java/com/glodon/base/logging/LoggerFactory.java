package com.glodon.base.logging;

import java.util.concurrent.ConcurrentHashMap;

import com.glodon.base.logging.impl.ConsoleLoggerFactory;
import com.glodon.base.logging.impl.Log4j2LoggerFactory;
import com.glodon.base.util.Utils;


/**
 * Created by liujing on 2023/10/12.
 */
public abstract class LoggerFactory {

    protected abstract Logger createLogger(String name);

    public static final String LOGGER_FACTORY_CLASS_NAME = "logger.factory";
    private static final ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<>();
    private static final LoggerFactory loggerFactory = getLoggerFactory();

    private static LoggerFactory getLoggerFactory() {
        String factoryClassName = null;
        try {
            factoryClassName = System.getProperty(LOGGER_FACTORY_CLASS_NAME);
        } catch (Exception e) {
        }
        if (factoryClassName != null) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try {
                Class<?> clz = loader.loadClass(factoryClassName);
                return Utils.newInstance(clz);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Error instantiating class \"" + factoryClassName + "\"", e);
            }
        } else if (LoggerFactory.class
                .getResource("/org/apache/logging/log4j/spi/ExtendedLogger.class") != null) {
            return new Log4j2LoggerFactory();
        } else {
            return new ConsoleLoggerFactory();
        }
    }

    public static Logger getLogger(Class<?> clazz) {
        String name = clazz.isAnonymousClass() ? clazz.getEnclosingClass().getCanonicalName()
                : clazz.getCanonicalName();
        return getLogger(name);
    }

    public static Logger getLogger(String name) {
        Logger logger = loggers.get(name);
        if (logger == null) {
            logger = loggerFactory.createLogger(name);
            Logger oldLogger = loggers.putIfAbsent(name, logger);
            if (oldLogger != null) {
                logger = oldLogger;
            }
        }
        return logger;
    }

    public static void removeLogger(String name) {
        loggers.remove(name);
    }
}
