package simpledb.storage;

import enums.LockType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import simpledb.LogUtils;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private static final int RETRY_MAX = 3;

    private static final int X_LOCK_WAIT = 100;
    private static final int S_LOCK_WAIT = 100;

    private final Integer numPages;
    private final Map<PageId, Page> pageCache;
    private final EvictStrategy evict;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        this.evict = new LRUEvict(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        LockType lockType;
        int retry = 2;
        // 根据不同的读写类型分发不同的锁
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SHARE_LOCK;
        } else {
            lockType = LockType.EXCLUSIVE_LOCK;
        }
        try {
            // 如果获取lock失败（重试RETRY_MAX次）则直接放弃事务
            if (!lockManager.acquireLock(pid, tid, lockType, retry)) {
                // 获取锁失败，回滚事务
                LogUtils.writeLog(LogUtils.ERROR, "tid:" + tid + "获取" + perm + "权限失败，进行回滚！！！");
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Method 「 getPage 」获取锁发生异常！！！");
        }
        if (!pageCache.containsKey(pid)) {
            if (pageCache.size() >= numPages) {
                System.out.println("准备置换" + pageCache.size());
                evictPage();
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            pageCache.put(pid, page);
            evict.addPageId(pid, page);
        }
        return pageCache.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        // 提交或者打断一个线程
        if (commit) {
            // 如果提交
            try {
                //
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 如果不提交，就重新重磁盘上读取该页面
        // 打断事务
        else {
            rollBack(tid);
        }
        lockManager.releasePagesByTid(tid);
    }

    private void rollBack(TransactionId tid) {
        // 將该事物对应的page页从磁盘中重新读取一次
        for (PageId pageId : pageCache.keySet()) {
            Page page = pageCache.get(pageId);
            if (tid.equals(page.isDirty())) {
                int tableId = pageId.getTableId();
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page readPage = table.readPage(pageId);
                pageCache.put(pageId, readPage);
                evict.updateByPageId(pageId, readPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(dbFile.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(dbFile.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pages, TransactionId tid) throws DbException {
        for (Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will`
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        pageCache.forEach((pageId, page) -> {
            try {
                flushPage(pageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page flush = pageCache.get(pid);
        TransactionId dirtier = flush.isDirty();
        if (dirtier != null) {
            // 在将Page刷入磁盘之前，先写入log日志，记录该page修改前后的值
            Database.getLogFile().logWrite(dirtier, flush.getBeforeImage(), flush);
            Database.getLogFile().force();
        }
        // 通过tableId找到对应的DbFile,并将page写入到对应的DbFile中
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        // 将page刷新到磁盘
        dbFile.writePage(flush);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // 将指定事务的Page全部写入磁盘中
        for (PageId pageId : pageCache.keySet()) {
            Page page = pageCache.get(pageId);
            TransactionId dirty = page.isDirty();
            Page beforeImage = page.getBeforeImage();
            page.setBeforeImage();
            if (dirty != null && dirty.equals(tid)) {
                Database.getLogFile().logWrite(tid, beforeImage, page);
                Database.getCatalog().getDatabaseFile(pageId.getTableId()).writePage(page);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId evictPageId = evict.getEvictPageId();
        try {
            flushPage(evictPageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pageCache.remove(evictPageId);
    }

    @AllArgsConstructor
    @Data
    private static class PageLock {
        private TransactionId tid;
        private PageId pid;
        private LockType type;

    }

    private static class LockManager {
        @Getter
        public ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         */
        public boolean holdsLock(TransactionId tid, PageId p) {
            // some code goes here
            // not necessary for lab1|lab2
            if (lockMap.get(p) == null) {
                return false;
            }
            return lockMap.get(p).get(tid) != null;
        }


        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType requestLock, int reTry) throws TransactionAbortedException, InterruptedException {
            // 重传达到3次
            if (reTry == RETRY_MAX) return false;
            // 用于打印log
            // 页面上不存在锁
            if (lockMap.get(pageId) == null) {
                return putLock(tid, pageId, requestLock);
            }

            // 页面上存在锁
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);

            if (tidLocksMap.get(tid) == null) {
                // 页面上的锁不是自己的
                // 请求的为X锁
                if (requestLock == LockType.EXCLUSIVE_LOCK) {
                    wait(X_LOCK_WAIT);
                    return acquireLock(pageId, tid, requestLock, reTry + 1);
                } else if (requestLock == LockType.SHARE_LOCK) {
                    // 页面上是否都是读锁 -> 页面上的锁大于1个，就都是读锁
                    // 因为排它锁只能被一个事务占有
                    if (tidLocksMap.size() > 1) {
                        // 都是读锁直接获取
                        return putLock(tid, pageId, requestLock);
                    } else {
                        Collection<PageLock> values = tidLocksMap.values();
                        for (PageLock value : values) {
                            // 存在的唯一的一个锁为X锁
                            if (value.getType() == LockType.EXCLUSIVE_LOCK) {
                                wait(S_LOCK_WAIT);
                                return acquireLock(pageId, tid, requestLock, reTry + 1);
                            } else {
                                // 说明存在的唯一一个锁也是读锁，直接获取
                                return putLock(tid, pageId, requestLock);
                            }
                        }
                    }
                }
                // 页面上的锁就是当前事务的
            } else {
                if (requestLock == LockType.SHARE_LOCK) {
                    tidLocksMap.remove(tid);
                    return putLock(tid, pageId, requestLock);
                } else {
                    // 判断自己的锁是否为排它锁，如果是直接获取
                    if (tidLocksMap.get(tid).getType() == LockType.EXCLUSIVE_LOCK) {
                        return true;
                    } else {
                        // 拥有的是读锁，判断是否还存在别的读锁
                        if (tidLocksMap.size() > 1) {
                            wait(S_LOCK_WAIT);
                            return acquireLock(pageId, tid, requestLock, reTry + 1);
                        } else {
                            // 只有自己拥有一个读锁，进行锁升级
                            tidLocksMap.remove(tid);
                            return putLock(tid, pageId, requestLock);
                        }
                    }
                }
            }
            return false;
        }

        public boolean putLock(TransactionId tid, PageId pageId, LockType requestLock) {
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            // 页面上一个锁都没
            if (tidLocksMap == null) {
                tidLocksMap = new ConcurrentHashMap<>();
                lockMap.put(pageId, tidLocksMap);
            }
            PageLock pageLock = new PageLock(tid, pageId, requestLock);
            tidLocksMap.put(tid, pageLock);
            lockMap.put(pageId, tidLocksMap);
            return true;
        }


        /**
         * 释放某个事务上所有页的锁
         */
        public synchronized void releasePagesByTid(TransactionId tid) {
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId : pageIds) {
                releasePage(tid, pageId);
            }
        }


        /**
         * 释放某个页上tid的锁
         */
        public synchronized void releasePage(TransactionId tid, PageId pid) {
            if (holdsLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, PageLock> tidLocks = lockMap.get(pid);
                tidLocks.remove(tid);
                if (tidLocks.size() == 0) {
                    lockMap.remove(pid);
                }
                // 释放锁时就唤醒正在等待的线程,因为wait与notifyAll都需要在同步代码块里，所以需要加synchronized
                this.notifyAll();
            }
        }
    }
}