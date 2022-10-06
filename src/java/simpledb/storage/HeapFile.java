package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
	public File f;
	public TupleDesc td;
	public RandomAccessFile rf;
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
        try{
            this.rf = new RandomAccessFile(f, "rw");
        }
        catch (Exception e)
        {
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
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
	    return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
	return td;
    }


    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
	//
	    byte[] b = new byte[BufferPool.getPageSize()];
	    try{
		    rf.seek((pid.getPageNumber()) * BufferPool.getPageSize());
		    rf.read(b);
		    return new HeapPage( new HeapPageId(this.getId(), pid.getPageNumber()), b);
	    }
	    catch (Exception e)
	    {
		    return null;
	    }

	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
	    byte[] b = ((HeapPage) page).getPageData();
        PageId pid = ((HeapPage) page).getId();
	    try{
		    rf.seek((pid.getPageNumber()) * BufferPool.getPageSize());

		    rf.write(b);
	    }
	    catch (Exception e)
	    {
	    }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) Math.floor(((int)f.length()) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        for (int i =0 ; i < numPages(); ++i)
        {

            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(), i), Permissions.READ_WRITE);	
            try{
                p.insertTuple(t);
                return Arrays.asList(p);
            }
            catch(Exception e)
            {

            }
        }
        int numPages = numPages();
        long currlen = rf.length();
        rf.setLength(rf.length() + BufferPool.getPageSize());
        rf.seek(currlen);
        for (int i = 0; i < BufferPool.getPageSize() ;++i)
        {
            rf.writeByte(0);
        }
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(), numPages), Permissions.READ_WRITE);	
        p.insertTuple(t);
        return Arrays.asList(p);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        int pgno = t.getRecordId().getPageId().getPageNumber();
        int tableid = t.getRecordId().getPageId().getTableId();
        if (tableid != getId())
        {
            throw new DbException("wrong table");
        }
        HeapPage p =(HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(), Permissions.READ_WRITE);	
        p.deleteTuple(t);
        return Arrays.asList(p);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIt(tid, this.numPages(), this.getId());
    }

}

