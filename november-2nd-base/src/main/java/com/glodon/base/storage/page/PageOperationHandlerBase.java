package com.glodon.base.storage.page;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.glodon.base.logging.Logger;

/**
 * Created by liujing on 2023/10/12.
 */
public abstract class PageOperationHandlerBase extends Thread implements PageOperationHandler {

    protected final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    protected final AtomicLong size = new AtomicLong();
    protected final int handlerId;
    protected final AtomicReferenceArray<PageOperationHandler> waitingHandlers;
    protected final AtomicBoolean hasWaitingHandlers = new AtomicBoolean(false);
    protected PageOperation lockedTask;

    public PageOperationHandlerBase(int handlerId, String name, int waitingQueueSize) {
        super(name);
        setDaemon(false);
        this.handlerId = handlerId;
        waitingHandlers = new AtomicReferenceArray<>(waitingQueueSize);
    }

    protected abstract Logger getLogger();

    @Override
    public int getHandlerId() {
        return handlerId;
    }

    @Override
    public long getLoad() {
        return size.get();
    }

    @Override
    public void handlePageOperation(PageOperation task) {
        size.incrementAndGet();
        pageOperations.add(task);
        wakeUp();
    }

    @Override
    public void addWaitingHandler(PageOperationHandler handler) {
        int id = handler.getHandlerId();
        if (id >= 0) {
            waitingHandlers.set(id, handler);
            hasWaitingHandlers.set(true);
        }
    }

    @Override
    public void wakeUpWaitingHandlers() {
        if (hasWaitingHandlers.compareAndSet(true, false)) {
            for (int i = 0, length = waitingHandlers.length(); i < length; i++) {
                PageOperationHandler handler = waitingHandlers.get(i);
                if (handler != null) {
                    handler.wakeUp();
                    waitingHandlers.compareAndSet(i, handler, null);
                }
            }
        }
    }

    protected void runPageOperationTasks() {
        PageOperation task;
        if (lockedTask != null) {
            task = lockedTask;
            lockedTask = null;
        } else {
            task = pageOperations.poll();
        }
        while (task != null) {
            try {
                PageOperation.PageOperationResult result = task.run(this);
                if (result == PageOperation.PageOperationResult.LOCKED) {
                    lockedTask = task;
                    break;
                } else if (result == PageOperation.PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                getLogger().warn("Failed to run page operation: " + task, e);
            }
            size.decrementAndGet();
            task = pageOperations.poll();
        }
    }
}
