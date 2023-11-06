package com.glodon.base.storage.page;

import com.glodon.base.conf.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liujing on 2023/10/12.
 */
public abstract class PageOperationHandlerFactory {

    protected PageOperationHandler[] pageOperationHandlers;

    protected PageOperationHandlerFactory(Config config, PageOperationHandler[] handlers) {
        if (handlers != null) {
            setPageOperationHandlers(handlers);
            return;
        }
        int handlerSize = config.getStoragePageOperationHandlerSize();
        pageOperationHandlers = new PageOperationHandler[handlerSize];
        for (int i = 0; i < handlerSize; i++) {
            pageOperationHandlers[i] = new DefaultPageOperationHandler(i, handlerSize, config);
        }
        startHandlers();
    }

    public abstract PageOperationHandler getPageOperationHandler();

    public PageOperationHandler[] getPageOperationHandlers() {
        return pageOperationHandlers;
    }

    public void setPageOperationHandlers(PageOperationHandler[] handlers) {
        pageOperationHandlers = new PageOperationHandler[handlers.length];
        System.arraycopy(handlers, 0, pageOperationHandlers, 0, handlers.length);
    }

    public void startHandlers() {
        for (PageOperationHandler h : pageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).startHandler();
            }
        }
    }

    public void stopHandlers() {
        for (PageOperationHandler h : pageOperationHandlers) {
            if (h instanceof DefaultPageOperationHandler) {
                ((DefaultPageOperationHandler) h).stopHandler();
            }
        }
    }

    public void addPageOperation(PageOperation po) {
        Object t = Thread.currentThread();
        if (t instanceof PageOperationHandler) {
            po.run((PageOperationHandler) t);
        } else {
            PageOperationHandler handler = getPageOperationHandler();
            handler.handlePageOperation(po);
        }
    }

    public static PageOperationHandlerFactory create(Config config) {
        return create(config, null);
    }

    public static synchronized PageOperationHandlerFactory create(Config config, PageOperationHandler[] handlers) {
        if (config == null) {
            config = Config.DEFAULT;
        }
        PageOperationHandlerFactory factory;
        String type = config.getStoragePageOperationHandlerFactoryType();
        if (type == null || type.equalsIgnoreCase("RoundRobin")) {
            factory = new RoundRobinFactory(config, handlers);
        } else if (type.equalsIgnoreCase("Random")) {
            factory = new RandomFactory(config, handlers);
        } else if (type.equalsIgnoreCase("LoadBalance")) {
            factory = new LoadBalanceFactory(config, handlers);
        } else {
            throw new RuntimeException("Unknow storage page operation handler factory type: " + type);
        }
        return factory;
    }

    private static class RandomFactory extends PageOperationHandlerFactory {

        private static final Random random = new Random();

        protected RandomFactory(Config config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            int index = random.nextInt(pageOperationHandlers.length);
            return pageOperationHandlers[index];
        }
    }

    private static class RoundRobinFactory extends PageOperationHandlerFactory {

        private static final AtomicInteger index = new AtomicInteger(0);

        protected RoundRobinFactory(Config config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            return pageOperationHandlers[index.getAndIncrement() % pageOperationHandlers.length];
        }
    }

    private static class LoadBalanceFactory extends PageOperationHandlerFactory {

        protected LoadBalanceFactory(Config config, PageOperationHandler[] handlers) {
            super(config, handlers);
        }

        @Override
        public PageOperationHandler getPageOperationHandler() {
            long minLoad = Long.MAX_VALUE;
            int index = 0;
            for (int i = 0, size = pageOperationHandlers.length; i < size; i++) {
                long load = pageOperationHandlers[i].getLoad();
                if (load < minLoad) {
                    index = i;
                }
            }
            return pageOperationHandlers[index];
        }
    }
}
