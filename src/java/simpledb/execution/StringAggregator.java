package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.*;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Tuple> results;
    private TupleDesc newdesc;
    private boolean finished;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
	if (what != Op.COUNT)
	{
		throw new IllegalArgumentException("can only use count on string");
	}
	finished = false;
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	this.what = what;
	if (gbfield == NO_GROUPING)
	{
		newdesc = new TupleDesc(new Type[]{Type.INT_TYPE});
	}
	else
	{
		newdesc = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
	}
	results = new HashMap<Field, Tuple>();
    }
    public TupleDesc getTupleDesc()
    {
	    return newdesc;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

	Field gf = gbfield == NO_GROUPING ? new IntField(1) : tup.getField(gbfield);
	if (!results.containsKey(gf))
	{
		results.put(gf, tup);	
		Tuple og_result = results.get(gf);
		og_result.setField(afield, new IntField(1)); 
		results.put(gf, og_result);
	}
	else
	{
		Tuple og_result = results.get(gf);
		
		IntField curr = (IntField) og_result.getField(afield);
		og_result.setField(afield,new  IntField( (curr.getValue() + 1)));
		results.put(gf, og_result);
	}
    }

    public HashMap<Field, Tuple> finish()
    {
	    HashMap<Field, Tuple> op = new HashMap<Field, Tuple>();
	for (Field k : results.keySet())
	{
		Tuple t = results.get(k);
		TupleDesc current = t.getTupleDesc();
		Tuple newtuple;
		if (gbfield == NO_GROUPING)
		{
			newtuple = new Tuple(newdesc);
			newtuple.setField(0, t.getField(afield));
		}
		else
		{
			newtuple = new Tuple(newdesc);
			newtuple.setField(1, t.getField(afield));
			newtuple.setField(0, t.getField(gbfield));
		}
		op.put(k, newtuple);
	}
	    return op;

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
	HashMap<Field, Tuple> t = finish();
	return new TupleIterator(newdesc, t.values());
    }

}
