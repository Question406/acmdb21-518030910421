package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator childIt;
    private int tableId;
    private TupleDesc td;
    private boolean didInsert;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.childIt = child;
        this.tableId = tableId;
        this.didInsert = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"InsertedNum"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        didInsert = false;
        childIt.open();
        super.open();
    }

    public void close() {
        // some code goes here
        didInsert = true;
        childIt.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        didInsert = false;
        childIt.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (didInsert)
            return null;
        didInsert = true;
        int insertCnt = 0;
        while (childIt.hasNext()) {
            Tuple item = childIt.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, item);
                ++insertCnt;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple resTup = new Tuple(td);
        resTup.setField(0, new IntField(insertCnt));
        return resTup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{childIt};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        assert children.length == 1;
        this.childIt = children[0];
    }
}
