package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    public Page[] pages;
    public boolean[] occupied;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    public int maxpages;
    public LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        System.out.println("BufferPool constructor");
        pages = new Page[numPages];
        occupied = new boolean[numPages];
        maxpages = numPages;
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
        boolean lock_granted = false;
        while (!lock_granted)
        {
            if(perm == Permissions.READ_ONLY)
            {
                lock_granted = lockManager.grantSharedLock(tid, pid);
            }
            else
            {
                lock_granted = lockManager.grantExclusiveLock(tid, pid);
            }
        }
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (!occupied[i])
            {
                continue;
            }
		   Page page = pages[i];
           if (page == null)
           {
            System.out.println("page is null");
            System.out.println(i);
            System.out.println(occupied[i]);
           }
		   if (pid.equals(page.getId()))
		   {

			   return page;
		   }

	    }
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (occupied[i])
            {
                continue;
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            if (page == null)
            {
                System.out.println("ballsack");
            }
            pages[i] = page;
            occupied[i] =true;
            // System.out.println("Returned a page here");
            return page;

	    }
        evictPage(); 
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (occupied[i])
            {
                continue;
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            if (page == null)
            {
                System.out.println("ballsack");
            }
            pages[i] = page;
            occupied[i] =true;
            return page;

	    }
        throw new TransactionAbortedException();
    }

    private void cachePage(Page cachepage, PageId pid)
            throws TransactionAbortedException, DbException {
	    for (int i = 0; i < pages.length; ++i)
	    {
           if (!occupied[i])
                {continue;}
		   Page page = pages[i];
		   if (page.getId().equals(pid))
		   {
                pages[i] = cachepage;
                return ;
		   }

	    }
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (occupied[i])
            {
                continue;
            }
            pages[i] = cachepage;
            occupied[i] =true;
            return;
	    }
		return ;
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
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLocks(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }


    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        // Flush dirty pages
        for (int i = 0; i < pages.length; ++i)
        {
            if (!occupied[i])
            {
                continue;
            }
            Page page = pages[i];
            if (page.isDirty() == tid)
            {
                try{
                    if (commit)
                    {
                        flushPage(page.getId());
                    }
                        
                    else
                        removePage(page.getId());
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        lockManager.releaseAllLocks(tid);
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
        // TODO: some code goes here
        // not necessary for lab1
        DbFile hf = (DbFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> q = hf.insertTuple(tid, t);
        for (Page p : q)
        {
            Page page = ((Page)p);
            ((Page)p).markDirty(true, tid);
            cachePage(page, ((Page)p).getId());
        }
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
        // TODO: some code goes here
        // not necessary for lab1
        DbFile hf = (DbFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> q = hf.deleteTuple(tid, t);
        for (Page p : q)
        {
            Page page = ((Page)p);
            ((Page)p).markDirty(true, tid);

            cachePage(page, ((Page)p).getId());
        }
        System.out.println("passed delete");
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1

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
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (!occupied[i])
            {
                continue;
            }
		   Page page = pages[i];
		   if (pid.equals(page.getId()))
		   {
                occupied[i] =false;
		   }

	    }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
	    for (int i = 0; i < pages.length; ++i)
	    {
            if (!occupied[i])
            {
                continue;
            }
		   Page page = pages[i];
		   if (pid.equals(page.getId()))
		   {
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(page);
		   }
	    }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
	    for (int i = 0; i < pages.length; ++i)
	    {
		   Page page = pages[i];
		   if (tid.equals(page.isDirty()))
		   {
                PageId pid = page.getId();
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(page);
		   }
	    }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1

	    for (int i = 0; i < pages.length; ++i)
	    {
            if (!occupied[i])
                continue;
            Page page = pages[i];
            if (page.isDirty() != null)
            {
                continue;
            }
            try{
                flushPage(page.getId());
                removePage(page.getId());
                return;
            }
            catch (Exception e)
            {
                System.out.println(e);
                
            }
	    }
        throw new DbException("no page to evict");
    }

}
