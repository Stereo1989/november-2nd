package com.glodon.base.async;

public class SucceededFuture<T> implements Future<T> {

    private final T result;

    public SucceededFuture(T result) {
        this.result = result;
    }

    @Override
    public T get() {
        return result;
    }

    @Override
    public T get(long timeoutMillis) {
        return result;
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        handler.handle(result);
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        handler.handle(new AsyncResult<>(result));
        return this;
    }
}
