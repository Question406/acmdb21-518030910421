package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate joinPredicate;
    private DbIterator childIt1;
    private DbIterator childIt2;
    private TupleDesc tupleDesc;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.childIt1 = child1;
        this.childIt2 = child2;
        this.tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public String getJoinField1Name()
    {
        // some code goes here
    	return childIt1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return childIt2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    }

    public void close() {
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{childIt1, childIt2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        assert children.length == 2;
        childIt1 = children[0];
        childIt2 = children[1];
    }

}
