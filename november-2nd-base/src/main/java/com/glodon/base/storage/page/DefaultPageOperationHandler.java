package com.glodon.base.storage.page;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.glodon.base.conf.Config;
import com.glodon.base.async.AsyncResult;
import com.glodon.base.logging.Logger;
import com.glodon.base.logging.LoggerFactory;
import com.glodon.base.util.ShutdownHookUtils;

/**
 * Created by liujing on 2023/10/12.
 */
public class DefaultPageOperationHandler extends PageOperationHandlerBase
        implements Runnable, PageOperation.Listener<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageOperationHandler.class);
    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stopped;
    private volatile boolean waiting;

    public DefaultPageOperationHandler(int id, int waitingQueueSize, Config config) {
        super(id, DefaultPageOperationHandler.class.getSimpleName() + "-" + id, waitingQueueSize);
        setDaemon(config.isEmbedded());
        loopInterval = config.getStoragePageOperationHandlerLoopInterval();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public void startHandler() {
        if (stopped) {
            return;
        }
        stopped = false;
        ShutdownHookUtils.addShutdownHook(getName(), () -> {
            stopHandler();
        });
        start();
    }

    public void stopHandler() {
        stopped = true;
        wakeUp();
    }

    @Override
    public void wakeUp() {
        if (waiting) {
            haveWork.release(1);
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            runPageOperationTasks();
            doAwait();
        }
    }

    private void doAwait() {
        waiting = true;
        try {
            haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
            haveWork.drainPermits();
        } catch (InterruptedException e) {
            logger.warn("", e);
        } finally {
            waiting = false;
        }
    }

    private volatile RuntimeException e;
    private volatile Object result;

    @Override
    public Object await() {
        e = null;
        result = null;
        while (result == null || e == null) {
            runPageOperationTasks();
            doAwait();
        }
        if (e != null) {
            throw e;
        }
        return result;
    }

    @Override
    public void handle(AsyncResult<Object> ar) {
        if (ar.isSucceeded()) {
            result = ar.getResult();
        } else {
            e = new RuntimeException(ar.getCause());
        }
        wakeUp();
    }
}
