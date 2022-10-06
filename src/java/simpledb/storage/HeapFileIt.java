
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
public class HeapFileIt extends AbstractDbFileIterator {
            private int i = -1;
	    private Iterator<Tuple> it;
	    private TransactionId tid;
	    private int numPages;
	    private int tableid;
	    private boolean open = false;

	    public HeapFileIt(TransactionId tid, int numPages, int t)
	    {
		    this.tid = tid;
		    this.numPages = numPages;
		    this.it = null;
		    tableid = t;
	    }
	    public void open()
	    {
		    open =true;
	    }
	    public void rewind()
	    {
		    i = -1;
		    it = null;
	    }
	    public void close()
	    {
		    super.close();
		    it = null;
		    i=-1;
		    open = false;
		    
		}

	public Tuple readNext()
	{
		if (!open) return null;
		if (it == null || it.hasNext() == false)
		{
			while ((1 + i) < numPages)
			{
				i++;
				HeapPageId id = new HeapPageId(tableid, i);
				try{
					it = ( (HeapPage)Database.getBufferPool().getPage(tid, id, Permissions.READ_ONLY)).iterator();
				} 
				catch (Exception e) {
				}
				if (it == null) continue;

				if (it.hasNext())
				{
					return it.next();
				}
			}
		}
		else if (it.hasNext())
		{
			return it.next();
		}
		return null;
	}
}


