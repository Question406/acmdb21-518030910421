package simpledb;

import java.io.*;

import java.util.*;

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
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int numPages;
    private Page[] pages = null;
    private HashMap<PageId, Integer> pageid2ind = null; // pageid to page index in bufferpool
    private LinkedList<PageId> LRUList = null; // first is the least recent used element
    private BitSet dirty = null; // use bitmap to mark which slot is dirty
    private BitSet empty = null; // use bitmap to mark which slot is empty


    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new Page[numPages];
        this.dirty = new BitSet(numPages);
        this.empty = new BitSet(numPages);
        this.empty.flip(0, numPages); // set all to empty
        this.pageid2ind = new HashMap<>();
        this.LRUList = new LinkedList<>();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (pageid2ind.containsKey(pid)) {
            // hit in buffer pool
            int ind = pageid2ind.get(pid);
            LRUList.remove(pid);
            LRUList.addLast(pid);
            dirty.set(ind);
            return pages[ind];
        }
        else {
            // not in buffer pool, collect it from disk
            // evict first then retrieve
            if (pageid2ind.size() == numPages)
                evictPage();
            // get empty index
            int emptyInd = empty.nextSetBit(0);
            assert (emptyInd >= 0 && emptyInd < numPages); // after evict, must have one empty slot

            DbFile belongingTable = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page retrievedPage = belongingTable.readPage(pid);
            pages[emptyInd] = retrievedPage;
            pageid2ind.put(pid, emptyInd);
            empty.clear(emptyInd); // nonemtpy
            dirty.set(emptyInd);
            LRUList.addLast(pid);  // LRU last
            return pages[emptyInd];
        }
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile toAddtable = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = toAddtable.insertTuple(tid, t);
        dirtyPages.forEach(dirtyPage -> dirtyPage.markDirty(true, tid));
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile toDelTable = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirtyPages = toDelTable.deleteTuple(tid, t);
        dirtyPages.forEach(dirtyPage -> dirtyPage.markDirty(true, tid));
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        pageid2ind.keySet().forEach(pid -> {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        int ind = pageid2ind.getOrDefault(pid, -1);
        if (ind != -1)  {
            if (dirty.get(ind)) {
                try {
                    flushPage(pid);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            empty.set(ind);
            LRUList.remove(pid);
            pageid2ind.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int ind = pageid2ind.getOrDefault(pid, -1);
        if (ind != -1) {
            Page toFlush = pages[ind];
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(toFlush);
            dirty.clear(ind); // mark as undirty, don't change empty since we don't evict it
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId toEvict = LRUList.pollFirst();
        int ind = pageid2ind.get(toEvict);
        // flush dirty page
        if (dirty.get(ind)) {
            try {
                flushPage(toEvict);
            } catch (IOException e) {
                throw new DbException("@evictPage IOException");
            }
        }
        assert (!empty.get(ind));
        pageid2ind.remove(toEvict);
        empty.set(ind);
    }

}
