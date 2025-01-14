package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.Catalog;
import simpledb.storage.*;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private Catalog catalog;
    private DbFile file;
    private DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
	    this.tid = tid;
	    this.tableid = tableid;
	    this.tableAlias = tableAlias;
	    this.catalog = Database.getCatalog();
	    it = null;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return catalog.getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.file = Database.getCatalog().getDatabaseFile(tableid) ;
        this.it = this.file.iterator(tid);
        this.it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc t = Database.getCatalog().getTupleDesc(tableid);
        Type[] t1 = new Type[t.numFields()];
        String[] t2 = new String[t.numFields()];
        for (int i = 0; i < t.numFields(); ++i)
        {
            t1[i] = t.getFieldType(i);
            t2[i] = tableAlias + "." + t.getFieldName(i);

        }
        return new TupleDesc(t1, t2);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (it == null)
        {
            return false;
        }
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (it == null)
        {
            return null;
        }
        // TODO: some code goes here
        return it.next();
    }

    public void close() {
	    if (this.it != null)
	    {this.it.close();}
	    this.it=null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
		    this.close();
		    this.open();
    }
}
