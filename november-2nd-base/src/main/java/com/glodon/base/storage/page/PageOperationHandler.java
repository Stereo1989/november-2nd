package com.glodon.base.storage.page;

/**
 * Created by liujing on 2023/10/12.
 */
public interface PageOperationHandler {

    int getHandlerId();

    long getLoad();

    void handlePageOperation(PageOperation po);

    void addWaitingHandler(PageOperationHandler handler);

    void wakeUpWaitingHandlers();

    void wakeUp();

    class DummyPageOperationHandler implements PageOperationHandler {
        @Override
        public int getHandlerId() {
            return -1;
        }

        @Override
        public long getLoad() {
            return 0;
        }

        @Override
        public void handlePageOperation(PageOperation po) {
        }

        @Override
        public void addWaitingHandler(PageOperationHandler handler) {
        }

        @Override
        public void wakeUpWaitingHandlers() {
        }

        @Override
        public void wakeUp() {
        }
    }
}
