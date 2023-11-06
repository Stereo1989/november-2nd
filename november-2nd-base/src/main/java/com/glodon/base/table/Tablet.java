package com.glodon.base.table;

import com.glodon.base.conf.Config;
import com.glodon.base.value.Value;

import java.util.List;
import java.util.Set;

/**
 * Created by liujing on 2023/10/19.
 */
public interface Tablet<ID_TYPE, DATA_TYPE extends Value> {

    void init(Config config);

    void close();

    void truncate();

    void drop();

    void checkpoint() throws InterruptedException;

    void insert(DATA_TYPE[] datas) throws InterruptedException;

    void insert(DATA_TYPE data);

    boolean update(DATA_TYPE data);

    boolean update(DATA_TYPE[] datas) throws InterruptedException;

    DATA_TYPE select(ID_TYPE id);

    void delete(ID_TYPE id);

    void delete(ID_TYPE[] ids) throws InterruptedException;

    void scan(Scanner<ID_TYPE, DATA_TYPE> scanner);

    Set<DATA_TYPE> in(Value v);
}
