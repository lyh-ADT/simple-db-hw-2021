package simpledb.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private final LockManager lockManager;

    private Map<PageId, Page> buffer;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        buffer = new ConcurrentHashMap<>();
        lockManager = new LockManager();
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
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, a page
     * should be evicted and the new page should be added in its place.
     * 
     * page会被transaction锁住，锁住的权限可以是读/读写。 获取page时需要判断是否被锁，被锁的话就阻塞住获取锁 pageId -> lock
     * pageId -> page
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        switch (perm) {
            case READ_ONLY:
                lockManager.acquireReadLock(pid, tid);
                break;
            case READ_WRITE:
                lockManager.acquireWriteLock(pid, tid);
                break;
        }

        Page page = buffer.get(pid);
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);

            // 有可能同时有多个相同pid的READ_ONLY线程进入到这里，需要避免多次淘汰
            // double-check
            if (buffer.size() == numPages) {
                synchronized (this) {
                    if (buffer.size() == numPages) {
                        evictPage();
                    }
                }
            }
            Page currentPage = buffer.putIfAbsent(pid, page);
            if (currentPage != null) {
                page = currentPage;
            }
        }

        return page;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        Set<PageId> writePages = lockManager.lockedWritePages(tid);
        if (commit) {
            try {
                for (PageId pageId : writePages) {
                    flushPage(pageId);
                    
                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    Page page = buffer.get(pageId);
                    if (page != null) {
                        page.setBeforeImage();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            writePages.forEach(buffer::remove);
        }
        writePages.forEach(pageId -> {
            lockManager.releaseLock(pageId, tid);
        });
        lockManager.lockedReadPages(tid).forEach(pageId -> lockManager.releaseLock(pageId, tid));
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        this.dirtyPages(dirtyPages, tid);
    }

    private void dirtyPages(List<Page> dirtyPages, TransactionId tid) {
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            buffer.put(page.getId(), page);
        }
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        this.dirtyPages(dirtyPages, tid);
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : buffer.values()) {
            if (page.isDirty() == null) {
                continue;
            }
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = buffer.get(pid);
        if (page == null || page.isDirty() == null) {
            // 没有脏数据，不用刷盘
            return;
        }

        // append an update record to the log, with 
        // a before-image and after-image.
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }

        page.markDirty(false, null);
        dbFile.writePage(page);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId evictId = buffer.keySet().stream().filter(k -> 
            buffer.get(k).isDirty() == null
        ).findAny().orElseThrow(()->new DbException("没有可以淘汰的缓存"));
        try {
            flushPage(evictId);
            buffer.remove(evictId);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DbException("IOException: " + e);
        }
    }

}
