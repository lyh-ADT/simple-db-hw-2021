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
    private TransactionId transactionId;
    private OpIterator iterator;
    private TupleDesc tupleDesc;
    private Integer deletedCount;

    /**
     * Constructor specifying the transaction that this delete belongs to as well as
     * the child to read from.
     * 
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.iterator = child;
        this.tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "deleted" });
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        this.deletedCount = 0;
        try {
            while (iterator.hasNext()) {
                Database.getBufferPool().deleteTuple(transactionId, iterator.next());
                this.deletedCount++;
            }
            iterator.close();
            super.open();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DbException("IOException:" + e);
        }
    }

    public void close() {
        deletedCount = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
        this.open();
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
        if (deletedCount == null) {
            return null;
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(deletedCount));
        deletedCount = null;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { iterator };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.iterator = children[0];
    }
}
