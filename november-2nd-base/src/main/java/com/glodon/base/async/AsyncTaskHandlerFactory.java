package com.glodon.base.async;

import java.util.concurrent.atomic.AtomicInteger;

public class AsyncTaskHandlerFactory {

    private static AsyncTaskHandler DEFAULT_HANDLER = new AsyncTaskHandler() {
        @Override
        public void handle(AsyncTask task) {
            task.run();
        }
    };

    private static final AtomicInteger index = new AtomicInteger(0);
    private static AsyncTaskHandler[] handlers = { DEFAULT_HANDLER };

    public static void setAsyncTaskHandlers(AsyncTaskHandler[] handlers) {
        AsyncTaskHandlerFactory.handlers = handlers;
    }

    public static AsyncTaskHandler getAsyncTaskHandler() {
        return handlers[index.getAndIncrement() % handlers.length];
    }
}
