package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private int noPg;
    private int fileID;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.fileID = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return fileID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int len = BufferPool.getPageSize();
        byte [] destBuf = new byte[len];
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            int offset = pid.pageNumber() * len;
            randomAccessFile.seek(offset);
            int bytesRead = randomAccessFile.read(destBuf, 0, len);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Page resPage = null;
        try {
            resPage = new HeapPage((HeapPageId) pid, destBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // write page to disk
        PageId pid = page.getId();
		RandomAccessFile rf = new RandomAccessFile(file, "rw");
		rf.seek(pid.pageNumber() * BufferPool.getPageSize());
		rf.write(page.getPageData(), 0, BufferPool.getPageSize());
		rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        noPg = (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
        return noPg;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if (! t.getTupleDesc().equals(td))
            throw new DbException("@HeapFile insertTuple, try to insert incorrect tupleDesc");
        // find page with empty slot
        int toInsPgNo = -1;
        HeapPage page = null;
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            page = (HeapPage) getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() != 0) {
                toInsPgNo = i;
                break;
            }
        }
        if (toInsPgNo != -1) {
            // there is a page with empty slot
            PageId pid = new HeapPageId(getId(), toInsPgNo);
            page = (HeapPage) getPage(tid, pid, Permissions.READ_WRITE);
        } else {
            // a new page needed
            page = (HeapPage) getEmptyPage(tid);
        }
        page.insertTuple(t);
        page.markDirty(true, tid);
        assert page != null;
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        // only one modified
        page.deleteTuple(t);
        // mark dirty
        page.markDirty(true, tid);
        return new ArrayList<Page>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        TransactionId tid;
        int pgInd = -1; // current page index in this file
        HeapPage curPg; // current page
        Iterator<Tuple> tpIt; // tuple iterator for current page

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        private HeapPage getHeapPage(int pageInd) throws TransactionAbortedException, DbException {
            return  (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageInd), Permissions.READ_ONLY);
        }

        /**
         * @param changeCur, whether change cur page in the iterator
         * @return
         * @throws TransactionAbortedException
         * @throws DbException
         */
        private boolean checkNext(boolean changeCur) throws TransactionAbortedException, DbException {
            if (curPg == null || tpIt == null)
                return false; // no next
            // this page not done
            if (tpIt.hasNext()) {
                return true; // has next && no need to change curPage, curIt
            }
            else {
                // change to next page
                int nxtInd = pgInd + 1;
                HeapPage nxtPage = null;
                Iterator<Tuple> nxtIt = null;
                while (nxtInd < numPages()) {
                    nxtPage = getHeapPage(nxtInd);
                    nxtIt = nxtPage.iterator();
                    if (nxtIt.hasNext()) {
                        Debug.log(String.valueOf(pgInd) + " : " + numPages());
                        if (changeCur) {
                            curPg = nxtPage;
                            tpIt = nxtIt;
                            pgInd = nxtInd;
                        }
                        return true; // has next && change curPage, curIt
                    }
                    ++nxtInd;
                }
                return false; // no next
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgInd = 0;
            curPg = getHeapPage(pgInd);
            tpIt = curPg.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            int tmpInd = 0;
            return checkNext(false);
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (! checkNext(true))
                throw new NoSuchElementException("@HeapFileIterator next\n");
            return tpIt.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgInd = -1;
            curPg = null;
            tpIt = null;
        }
    }

    Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {
        Page p = Database.getBufferPool().getPage(tid, pid, perm);
		return p;
	}

    private Page getEmptyPage(TransactionId tid)
			throws DbException, IOException, TransactionAbortedException {
		// create the new page
		int emptyPageNo = numPages();
        HeapPageId pid = new HeapPageId(getId(), emptyPageNo);
        HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());

        writePage(newPage);
		// make sure the page is not in the buffer pool	or in the local cache
		Database.getBufferPool().discardPage(newPage.getId());
		return getPage(tid, pid, Permissions.READ_WRITE);
	}

}

