package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int aggregateFieldIndex;
    private int groupByFieldIndex;
    private Op operation;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggregateFieldIndex = afield;
        this.groupByFieldIndex = gfield;
        this.operation = aop;

        TupleDesc childTd = child.getTupleDesc();
        if (gfield == Aggregator.NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[] { childTd.getFieldType(afield) },
                    new String[] { String.format("%s(%s)", aop.name(), aggregateFieldName()) });
        } else {
            this.tupleDesc = new TupleDesc(new Type[] { childTd.getFieldType(gfield), childTd.getFieldType(afield) },
                    new String[] { null, String.format("%s(%s)", aop.name(), aggregateFieldName()) });
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of
     *         the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
     */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(groupByFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return operation;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();

        this.getAggregator();

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        iterator = aggregator.iterator();
        iterator.open();
        super.open();
    }

    private void getAggregator() {
        TupleDesc childTd = child.getTupleDesc();
        Type groupByType = groupByFieldIndex == Aggregator.NO_GROUPING ? null : childTd.getFieldType(groupByFieldIndex);
        switch (childTd.getFieldType(aggregateFieldIndex)) {
            case INT_TYPE:
                this.aggregator = new IntegerAggregator(groupByFieldIndex, groupByType, aggregateFieldIndex, operation);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(groupByFieldIndex, groupByType, aggregateFieldIndex, operation);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is
     * the field by which we are grouping, and the second field is the result of
     * computing the aggregate. If there is no group by field, then the result tuple
     * should contain one field representing the result of the aggregate. Should
     * return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!iterator.hasNext()) {
            return null;
        }
        Tuple next = iterator.next();
        return next;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this
     * will have one field - the aggregate column. If there is a group by field, the
     * first field will be the group by field, and the second will be the aggregate
     * value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
     * in the constructor, and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        super.close();
        if (iterator == null) {
            return;
        }
        iterator.close();
        iterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
