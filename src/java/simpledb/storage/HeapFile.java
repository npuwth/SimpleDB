package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;

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
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int index = BufferPool.getPageSize()*pid.getPageNumber();
        byte[] data = HeapPage.createEmptyPageData();
        try {
            FileInputStream in = new FileInputStream(this.file);
            assert in.skip(index) == index;
            assert in.read(data, 0, BufferPool.getPageSize()) == BufferPool.getPageSize();
            in.close();
            HeapPageId hpid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(hpid, data);
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
        } catch (IOException e) {
            System.out.println("IOException:" + e.toString());
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long totalLen = this.file.length();
        return (int) totalLen / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // iterate through the tuples of each HeapPage in the HeapFile
    private static class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;

        private final int tableId;

        private final int pageNum;

        private int pgCursor;

        Iterator<Tuple> tupleIt;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.tid = tid;
            this.tableId = file.getId();
            this.pageNum = file.numPages();
            this.tupleIt = null;
            this.pgCursor = 0;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pgCursor = 0;
            this.tupleIt = getTupleIt(0);
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(this.tupleIt != null && this.tupleIt.hasNext()) return true;
            else {
                this.pgCursor++;
                while(this.pgCursor < this.pageNum) {
                    this.tupleIt = getTupleIt(this.pgCursor);
                    if(this.tupleIt != null && this.tupleIt.hasNext()) return true;
                    this.pgCursor++;
                }
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(hasNext()) return this.tupleIt.next();
            else throw new NoSuchElementException("No More Elements!");
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            this.pgCursor = 0;
            this.tupleIt = null;
        }

        // get the HeapPage's tuple iterator
        private Iterator<Tuple> getTupleIt(int cursor) throws TransactionAbortedException, DbException {
            HeapPageId hpid = new HeapPageId(this.tableId, cursor);
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, Permissions.READ_ONLY);
            return pg.iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

