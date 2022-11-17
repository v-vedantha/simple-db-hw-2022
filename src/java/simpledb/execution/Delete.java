package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean called;
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        this.t=t;
        this.child=child;
        this.tableId=tableId;
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (called)
        {
            return null;
        }
        called = true;
        int tot = 0;
        while(child.hasNext())
        {
            tot++;
            Tuple tup = child.next();
            try{
            Database.getBufferPool().deleteTuple(t, tup);
            }
            catch (IOException e)
            {
                tot--;

            }
        }
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        Tuple nt = new Tuple(td);
        nt.setField(0, new IntField(tot));
        return nt;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
