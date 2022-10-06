package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    private Predicate p;
    private OpIterator child;
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
	this.p = p;
	this.child = child;
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
	this.child.open();
	super.open();
    }

    public void close() {
        // TODO: some code goes here
	this.child.close();
	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
	this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
	while (this.child.hasNext())
	{
		Tuple next = this.child.next();
		if (p.filter(next))
		{
			return next;
		}
	}
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
	this.child = children[0];
    }

}
