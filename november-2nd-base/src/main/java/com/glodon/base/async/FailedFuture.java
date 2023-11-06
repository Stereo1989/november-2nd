package com.glodon.base.async;

import com.glodon.base.exceptions.UnificationException;

public class FailedFuture<T> implements Future<T> {

    private final Throwable cause;

    public FailedFuture(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public T get() {
        throw UnificationException.convert(cause);
    }

    @Override
    public T get(long timeoutMillis) {
        return get();
    }

    @Override
    public Future<T> onSuccess(AsyncHandler<T> handler) {
        return this;
    }

    @Override
    public Future<T> onFailure(AsyncHandler<Throwable> handler) {
        handler.handle(cause);
        return this;
    }

    @Override
    public Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler) {
        handler.handle(new AsyncResult<>(cause));
        return this;
    }
}
