package com.glodon.base.storage.page;

import com.glodon.base.async.AsyncHandler;
import com.glodon.base.async.AsyncResult;
import com.glodon.base.async.AsyncTask;

import java.util.concurrent.CountDownLatch;

/**
 * Created by liujing on 2023/10/12.
 */
public interface PageOperation extends AsyncTask {

    enum PageOperationResult {
        SPLITTING,
        SUCCEEDED,
        SHIFTED,
        REMOTE_WRITTING,
        RETRY,
        FAILED,
        LOCKED;
    }

    @Override
    default int getPriority() {
        return MAX_PRIORITY;
    }

    @Override
    default void run() {
    }

    default PageOperationResult run(PageOperationHandler currentHandler) {
        run();
        return PageOperationResult.SUCCEEDED;
    }

    interface Listener<V> extends AsyncHandler<AsyncResult<V>> {

        default void startListen() {
        }

        V await();
    }

    interface ListenerFactory<V> {
        Listener<V> createListener();
    }

    class SyncListener<V> implements Listener<V> {

        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile RuntimeException e;
        private volatile V result;

        @Override
        public V await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                this.e = new RuntimeException(e);
            }
            if (e != null)
                throw e;
            return result;
        }

        @Override
        public void handle(AsyncResult<V> ar) {
            if (ar.isSucceeded())
                result = ar.getResult();
            else
                e = new RuntimeException(ar.getCause());
            latch.countDown();
        }
    }
}
