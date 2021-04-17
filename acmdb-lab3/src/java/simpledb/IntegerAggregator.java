package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;

    // (group value, aggregate value)
    private HashMap<Field, Integer> groupRes;
    private HashMap<Field, Integer> groupCnt; // for COUNT && AVG operator

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupRes = new HashMap<>();
        this.groupCnt = new HashMap<>();
        if (gbfield == NO_GROUPING)
            // no grouping
            // seems no check on tupleDesc
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"IntegerAggregateRes"});
        else {
            // need grouping
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"IntegerGroupRes", "IntegerAggregateRes"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        Integer toPut, curRes, curCnt;
        if (gbfield == NO_GROUPING) {
            toPut = ((IntField) tup.getField(afield)).getValue();
            // only AVG is different, NOTE: COUNT is the same since always return 1
            if (what == Op.AVG) {
                curRes = groupRes.getOrDefault(groupKey, 0);
                curCnt = groupCnt.getOrDefault(groupKey, 1);
                groupRes.put(groupKey, toPut + curRes);
                groupCnt.put(groupKey, curCnt + 1);
            } else {
                groupRes.put(groupKey, toPut);
                groupCnt.put(groupKey, 1);
            }
        } else {
            curRes = groupRes.get(groupKey);
            curCnt = groupCnt.get(groupKey);
            toPut = ((IntField) tup.getField(afield)).getValue();
            switch (what) {
                case MIN -> {
                    toPut = (curRes == null) ? toPut : Math.min(curRes, toPut);
                    groupRes.put(groupKey, toPut);
                }
                case MAX -> {
                    toPut = (curRes == null) ? toPut : Math.max(curRes, toPut);
                    groupRes.put(groupKey, toPut);
                }
                case SUM -> {
                    toPut = (curRes == null) ? toPut : curRes + toPut;
                    groupRes.put(groupKey, toPut);
                }
                case AVG -> {
                    toPut = (curRes == null) ? toPut : curRes + toPut;
                    groupRes.put(groupKey, toPut);
                    toPut = (curCnt == null) ? 1 : curCnt + 1;
                    groupCnt.put(groupKey, toPut);
                }
                case COUNT -> {
                    toPut = (curCnt == null) ? 1 : curCnt + 1;
                    groupCnt.put(groupKey, toPut);
                }
                case SUM_COUNT -> {
                    throw new IllegalArgumentException("@IntegerAggregator, not imlemented operator");
                }
                case SC_AVG -> {
                    throw new IllegalArgumentException("@IntegerAggregator, not imlemented operator");
                }
            };
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @param key group key field
     * @return the result field of this group
     */
    private Field getAggRes(Field key){
        if (what == Op.SC_AVG || what == Op.SUM_COUNT)
            throw new IllegalArgumentException("@IntegerAggregator, not implemented operator");
        if (what == Op.COUNT)
           return new IntField(groupCnt.get(key));
        else if (what == Op.AVG)
            return new IntField(groupRes.get(key) / groupCnt.get(key));
        else
            return new IntField(groupRes.get(key));
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> resTups = new ArrayList<Tuple>();
        HashMap<Field, Integer> candidate = groupRes;
        if (what == Op.COUNT)
            candidate = groupCnt;
        for (Field groupKey : candidate.keySet()) {
            Field aggRes = getAggRes(groupKey);
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

}
