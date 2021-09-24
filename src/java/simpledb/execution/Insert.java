package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator iterator;
    private DbFile tableFile;
    private Integer insertedCount;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        if (!tableFile.getTupleDesc().equals(child.getTupleDesc())) {
            throw new DbException("child's tupleDesc and table's not the same");
        }
        this.transactionId = t;
        this.iterator = child;
        this.tableFile = tableFile;

        this.tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "inserted" });

    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        this.insertedCount = 0;
        try {
            while (iterator.hasNext()) {
                Database.getBufferPool().insertTuple(transactionId, tableFile.getId(), iterator.next());
                this.insertedCount++;
            }
            iterator.close();
            super.open();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DbException("IOException:" + e);
        }
    }

    public void close() {
        this.insertedCount = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor.
     * It returns a one field tuple containing the number of inserted records.
     * Inserts should be passed through BufferPool. An instances of BufferPool is
     * available via Database.getBufferPool(). Note that insert DOES NOT need check
     * to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if
     *         called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (insertedCount == null) {
            return null;
        }
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0, new IntField(this.insertedCount));
        insertedCount = null;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { iterator };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        iterator = children[0];
    }
}
