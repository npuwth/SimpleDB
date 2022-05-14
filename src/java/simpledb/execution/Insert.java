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

import java.io.Serial;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private final TransactionId tid;

    private OpIterator child;

    private final int tableId;

    private boolean fetched;

    @Serial
    private static final long serialVersionUID = 1L;

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
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc differs between child and target!");
        }
        this.fetched = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
//        return Database.getCatalog().getTupleDesc(tableId);
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
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
        int cnt = 0;
        while(child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                cnt++;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Cannot insert tuple on table:" + tableId + " with transaction:" + tid + "!");
            }
        }
        if(fetched) return null;
        else {
            fetched = true;
            Tuple rVal = new Tuple(new TupleDesc(new Type[] { Type.INT_TYPE }));
            rVal.setField(0, new IntField(cnt));
            return rVal;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
