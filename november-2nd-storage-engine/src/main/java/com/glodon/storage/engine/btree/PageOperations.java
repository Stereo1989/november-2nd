package com.glodon.storage.engine.btree;

import com.glodon.base.async.AsyncHandler;
import com.glodon.base.async.AsyncResult;
import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.value.ValueLong;
import com.glodon.base.storage.page.PageOperation;
import com.glodon.base.storage.page.PageOperationHandler;

/**
 * 数据页操作基类
 */
public abstract class PageOperations {

    private PageOperations() {
    }

    /**
     * 单一写
     *
     * @param <K>
     * @param <V>
     * @param <R>
     */
    public static abstract class SingleWrite<K, V, R> implements PageOperation {
        final BTreeMap<K, V> map;
        K key;
        AsyncHandler<AsyncResult<R>> resultHandler;

        Page p;
        PageReference pRef;
        Object result;

        ChildOperation childOperation;

        public SingleWrite(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<R>> resultHandler) {
            this.map = map;
            this.key = key;
            this.resultHandler = resultHandler;
        }

        // 可以延后设置
        public void setResultHandler(AsyncHandler<AsyncResult<R>> resultHandler) {
            this.resultHandler = resultHandler;
        }

        public AsyncHandler<AsyncResult<R>> getResultHandler() {
            return resultHandler;
        }

        @SuppressWarnings("unchecked")
        public R getResult() {
            return (R) result;
        }

        @Override
        public PageOperationResult run(PageOperationHandler poHandler) {
            if (p == null) {
                p = gotoLeafPage();
                pRef = p.getRef();
            }

            if (pRef.page.isNode() || pRef.isDataStructureChanged()) {
                p = null;
                return PageOperationResult.RETRY;
            }

            if (childOperation != null) {
                return runChildOperation(poHandler);
            }
            if (pRef.tryLock(poHandler)) {
                if (pRef.page.isNode() || pRef.isDataStructureChanged()) {
                    p = null;
                    pRef.unlock();
                    return PageOperationResult.RETRY;
                }
                p = pRef.page;
                write(poHandler);
                if (childOperation != null) {
                    return runChildOperation(poHandler);
                } else {
                    return handleAsyncResult();
                }
            } else {
                return PageOperationResult.LOCKED;
            }
        }

        private void write(PageOperationHandler poHandler) {
            int index = getKeyIndex();
            result = writeLocal(index);

            if (index < 0 && p.needSplit()) {
                childOperation = splitLeafPage(p);
            }
        }

        private PageOperationResult runChildOperation(PageOperationHandler poHandler) {
            if (childOperation.run(poHandler)) {
                childOperation = null;
                pRef.setDataStructureChanged(true);
                return handleAsyncResult();
            }
            return PageOperationResult.LOCKED;
        }

        @SuppressWarnings("unchecked")
        private PageOperationResult handleAsyncResult() {
            pRef.unlock();
            if (resultHandler != null)
                resultHandler.handle(new AsyncResult<>((R) result));
            return PageOperationResult.SUCCEEDED;
        }

        protected abstract Object writeLocal(int index);

        protected void insertLeaf(int index, V value) {
            //复制并插入新的v
            index = -index - 1;
            p = p.copyLeaf(index, key, value);
            p.getRef().replacePage(p);
            map.setMaxKey(key);
        }

        protected void markDirtyPages() {
            p.markDirtyRecursive();
        }

        //标记脏页
        protected abstract boolean isMarkDirtyEnabled();

        protected Page gotoLeafPage() {
            return map.gotoLeafPage(key, isMarkDirtyEnabled());
        }

        protected int getKeyIndex() {
            return p.binarySearch(key);
        }
    }

    public static class Put<K, V, R> extends SingleWrite<K, V, R> {
        final V value;

        public Put(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<R>> resultHandler) {
            super(map, key, resultHandler);
            this.value = value;
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return true;
        }

        @Override
        protected Object writeLocal(int index) {
            p.markDirty(true);
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                return p.setValue(index, value);
            }
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value,
                           AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, value, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Object writeLocal(int index) {
            if (index < 0) {
                markDirtyPages();
                insertLeaf(index, value);
                return null;
            }
            return p.getValue(index);
        }
    }

    public static class Append<K, V> extends Put<K, V, K> {

        public Append(BTreeMap<K, V> map, V value, AsyncHandler<AsyncResult<K>> resultHandler) {
            super(map, null, value, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            throw UnificationException.getInternalError();
        }

        @Override
        protected Page gotoLeafPage() {
            Page p = map.getRootPage();
            while (true) {
                if (p.isLeaf()) {
                    return p;
                }
                p.markDirty();
                p = p.getChildPage(map.getChildPageCount(p) - 1);
            }
        }

        @Override
        protected int getKeyIndex() {
            return -(p.getKeyCount() + 1);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Object writeLocal(int index) {
            key = (K) ValueLong.get(map.incrementAndGetMaxKey());
            p.markDirty(true);
            insertLeaf(index, value);
            return key;
        }
    }

    public static class Replace<K, V> extends Put<K, V, Boolean> {
        private final V oldValue;

        public Replace(BTreeMap<K, V> map, K key, V oldValue, V newValue,
                       AsyncHandler<AsyncResult<Boolean>> resultHandler) {
            super(map, key, newValue, resultHandler);
            this.oldValue = oldValue;
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Boolean writeLocal(int index) {
            if (index < 0) {
                return Boolean.FALSE;
            }
            Object old = p.getValue(index);
            if (map.areValuesEqual(old, oldValue)) {
                markDirtyPages();
                p.setValue(index, value);
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }
    }

    public static class Remove<K, V> extends SingleWrite<K, V, V> {

        public Remove(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Object writeLocal(int index) {
            if (index < 0) {
                return null;
            }
            markDirtyPages();
            Object oldValue = p.getValue(index);
            Page oldRootPage = map.getRootPage();
            Page newPage = p.copy();
            newPage.remove(index);
            p.getRef().replacePage(newPage);
            if (newPage.isEmpty() && p != oldRootPage) {
                childOperation = new RemoveChild(p, key);
            }
            return oldValue;
        }
    }

    private static interface ChildOperation {
        public boolean run(PageOperationHandler poHandler);
    }

    private static class AddChild implements ChildOperation {
        private TmpNodePage tmpNodePage;
        private int count;

        public AddChild(TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
        }

        @Override
        public boolean run(PageOperationHandler poHandler) {
            return insertChildren(poHandler, tmpNodePage) && count == 0;
        }

        private boolean insertChildren(PageOperationHandler poHandler, TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
            Page parent = tmpNodePage.old.getParentRef().page;
            Page old = parent;
            if (!old.getRef().tryLock(poHandler))
                return false;
            count++;
            PageReference parentRef = parent.getRef();
            int index = parent.getPageIndex(tmpNodePage.key);
            parent = parent.copy();
            parent.setAndInsertChild(index, tmpNodePage);
            parentRef.replacePage(parent);

            if (parent.needSplit()) {
                TmpNodePage tmp = splitPage(parent);
                for (PageReference ref : tmp.left.page.getChildren()) {
                    if (ref.page != null) {
                        ref.page.setParentRef(tmp.left.page.getRef());
                    }
                }
                for (PageReference ref : tmp.right.page.getChildren()) {
                    if (ref.page != null) {
                        ref.page.setParentRef(tmp.right.page.getRef());
                    }
                }
                if (parent.getParentRef() == null) {
                    tmp.left.page.setParentRef(tmp.parent.getRef());
                    tmp.right.page.setParentRef(tmp.parent.getRef());
                    parent.bTreeMap.newRoot(tmp.parent);
                } else {
                    insertChildren(poHandler, tmp);
                }
            } else {
                if (parent.getParentRef() == null)
                    parent.bTreeMap.newRoot(parent);
            }
            count--;
            old.getRef().unlock();
            return true;
        }
    }

    private static class RemoveChild implements ChildOperation {
        private final Page old;
        private final Object key;
        private int count;

        public RemoveChild(Page old, Object key) {
            this.old = old;
            this.key = key;
        }

        @Override
        public boolean run(PageOperationHandler poHandler) {
            Page root = old.bTreeMap.getRootPage();
            if (!root.isNode()) {
                throw UnificationException.getInternalError();
            }
            Page p = remove(poHandler, root, key);
            boolean ok = p != null;
            if (ok && count == 0) {
                if (p.isEmpty()) {
                    p = BTreeLeaf.createEmpty(old.bTreeMap);
                }
                old.bTreeMap.newRoot(p);
                return true;
            }
            return false;
        }

        private Page remove(PageOperationHandler poHandler, Page p, Object key) {
            int index = p.getPageIndex(key);
            Page c = p.getChildPage(index);
            Page cOld = c;
            if (c.isNode()) {
                c = remove(poHandler, c, key);
                if (c == null)
                    return null;
            }
            if (c.isNotEmpty()) {
                if (cOld != c)
                    cOld.getRef().replacePage(c);
            } else {
                PageReference ref = p.getRef();
                if (!ref.tryLock(poHandler))
                    return null;
                count++;
                p = p.copy();
                p.remove(index);
                ref.replacePage(p);
                count--;
                ref.unlock();
            }
            return p;
        }
    }

    public static class TmpNodePage {
        final Page parent;
        final Page old;
        final PageReference left;
        final PageReference right;
        final Object key;

        public TmpNodePage(Page parent, Page old, PageReference left, PageReference right, Object key) {
            this.parent = parent;
            this.old = old;
            this.left = left;
            this.right = right;
            this.key = key;
        }
    }

    private static AddChild splitLeafPage(Page p) {
        TmpNodePage tmp = splitPage(p);

        if (p == p.bTreeMap.getRootPage()) {
            tmp.left.page.setParentRef(tmp.parent.getRef());
            tmp.right.page.setParentRef(tmp.parent.getRef());
            p.bTreeMap.newRoot(tmp.parent);
            return null;
        }

        tmp.left.page.setParentRef(p.getParentRef());
        tmp.right.page.setParentRef(p.getParentRef());

        return new AddChild(tmp);
    }

    private static TmpNodePage splitPage(Page p) {
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        Page old = p;
        p = p.copy();
        Page rightChildPage = p.split(at);
        Page leftChildPage = p;
        PageReference leftRef = new PageReference(leftChildPage);
        PageReference rightRef = new PageReference(rightChildPage);
        Object[] keys = {k};
        PageReference[] children = {leftRef, rightRef};
        Page parent = BTreeNode.create(p.bTreeMap, keys, children, 0);
        PageReference parentRef = new PageReference(parent);
        parent.setRef(parentRef);
        leftChildPage.setRef(leftRef);
        rightChildPage.setRef(rightRef);
        return new TmpNodePage(parent, old, leftRef, rightRef, k);
    }
}
