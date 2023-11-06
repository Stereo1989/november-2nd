package com.glodon.base.async;

public interface Future<T> {

    static <T> Future<T> succeededFuture(T result) {
        return new SucceededFuture<>(result);
    }

    static <T> Future<T> failedFuture(Throwable t) {
        return new FailedFuture<>(t);
    }

    T get();

    T get(long timeoutMillis);

    Future<T> onSuccess(AsyncHandler<T> handler);

    Future<T> onFailure(AsyncHandler<Throwable> handler);

    Future<T> onComplete(AsyncHandler<AsyncResult<T>> handler);
}
