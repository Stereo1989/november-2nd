package com.glodon.base.async;

public interface AsyncTaskHandler extends AsyncHandler<AsyncTask> {

    @Override
    void handle(AsyncTask task);

    default void addPeriodicTask(AsyncPeriodicTask task) {
    }

    default void removePeriodicTask(AsyncPeriodicTask task) {
    }
}
