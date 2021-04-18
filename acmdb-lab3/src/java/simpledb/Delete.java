package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator childIt;
    private boolean didDelete;
    private TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.childIt = child;
        this.didDelete = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"DeletedNum"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        didDelete = false;
        childIt.open();
        super.open();
    }

    public void close() {
        // some code goes here
        didDelete = true;
        childIt.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childIt.rewind();
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
        // some code goes here
        if (this.didDelete)
            return null;
        didDelete = true;
        int delCnt = 0;
        while (childIt.hasNext()) {
            Tuple item = childIt.next();
            try {
                Database.getBufferPool().deleteTuple(tid, item);
                ++delCnt;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple resTup = new Tuple(td);
        resTup.setField(0, new IntField(delCnt));
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
        childIt = children[0];
    }

}
