package com.glodon.base.table;

/**
 * Created by liujing on 2023/10/20.
 */
public interface Scanner<K,V> {

    void handle(K k,V v);

}
