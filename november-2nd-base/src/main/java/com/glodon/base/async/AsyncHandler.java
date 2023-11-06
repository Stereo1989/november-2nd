package com.glodon.base.async;

public interface AsyncHandler<E> {

    void handle(E event);
}