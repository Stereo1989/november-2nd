package com.glodon.storage.engine.btree;

public class CursorPos {

    public final Page page;
    public int index;

    public final CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }
}
