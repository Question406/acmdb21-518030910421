package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private TupleDesc tupleDesc;
    private HashMap<Field, Integer> groupRes;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("@StringAggregator");
        this.what = what;
        this.groupRes = new HashMap<>();
        if (gbfield == NO_GROUPING)
            // no grouping
            // seems no check on tupleDesc
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"StringAggregateRes"});
        else {
            // need grouping
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"StringGroupRes", "StringAggregateRes"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        Integer toPut;
        if (gbfield == NO_GROUPING) {
            toPut = ((IntField) tup.getField(afield)).getValue();
            groupRes.put(groupKey, toPut);
        } else {
            toPut = groupRes.get(groupKey);
            toPut = (toPut == null) ? 1 : toPut + 1;
            groupRes.put(groupKey, toPut);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> resTups = new ArrayList<Tuple>();
        for (Field groupKey : this.groupRes.keySet()) {
            Field aggRes = new IntField(groupRes.get(groupKey));
            Tuple t = new Tuple(tupleDesc);
            if (gbfield == NO_GROUPING) {
                // no grouping
                t.setField(0, aggRes);
            } else {
                // need grouping
                t.setField(0, groupKey);
                t.setField(1, aggRes);
            }
            resTups.add(t);
        }
        return new TupleIterator(tupleDesc, resTups);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }
}
