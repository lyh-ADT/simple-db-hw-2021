package simpledb.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.UniqueField;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldIndex;
    private Type groupByFieldType;
    private int aggregateFieldIndex;
    private Op operation;
    private AbstractIntegerAggregator aggregator;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.operation = what;
        this.chooseAggregator();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gField;
        if (this.groupByFieldIndex == NO_GROUPING) {
            gField = UniqueField.getInstance();
        } else {
            gField = tup.getField(this.groupByFieldIndex);
        }
        IntField aField = (IntField) tup.getField(this.aggregateFieldIndex);
        this.aggregator.apply(gField, aField.getValue());
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
     *         using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {
        Map<Field, Integer> result = this.aggregator.result();
        return new Iterator(groupByFieldType, result);
    }

    private void chooseAggregator() {
        switch (operation) {
            case AVG:
                this.aggregator = new IntegerAverageAggregator();
                break;
            case COUNT:
                this.aggregator = new IntegerCountAggregator();
                break;
            case MAX:
                this.aggregator = new IntegerMaxAggregator();
                break;
            case MIN:
                this.aggregator = new IntegerMinAggregator();
                break;
            case SUM:
                this.aggregator = new IntegerSumAggregator();
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static class Iterator implements OpIterator {
        private Map<Field, Integer> result;
        private java.util.Iterator<Entry<Field, Integer>> iterator;
        private TupleDesc tupleDesc;

        public Iterator(Type groupByFieldType, Map<Field, Integer> result) {
            this.result = result;
            if (result.containsKey(UniqueField.getInstance())) {
                this.tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
            } else {
                this.tupleDesc = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = result.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            Entry<Field, Integer> entry = iterator.next();
            Tuple tuple = new Tuple(tupleDesc);
            if (UniqueField.getInstance() == entry.getKey()) {
                tuple.setField(0, new IntField(entry.getValue()));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            if (iterator == null) {
                return;
            }
            iterator = null;
        }

    }
}

abstract class AbstractIntegerAggregator {
    protected Map<Field, Integer> groups = new HashMap<>();

    public abstract void apply(Field group, Integer value);

    public Map<Field, Integer> result() {
        return groups;
    }
}

class IntegerMinAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (g, oldValue) -> {
            if (oldValue == null) {
                oldValue = Integer.MAX_VALUE;
            }
            return Math.min(oldValue, value);
        });
    }
}

class IntegerMaxAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (g, oldValue) -> {
            if (oldValue == null) {
                oldValue = Integer.MIN_VALUE;
            }
            return Math.max(oldValue, value);
        });
    }
}

class IntegerSumAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (g, oldValue) -> {
            if (oldValue == null) {
                oldValue = 0;
            }
            return oldValue + value;
        });
    }
}

class IntegerCountAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (g, oldValue) -> {
            if (oldValue == null) {
                oldValue = 0;
            }
            return oldValue + 1;
        });
    }
}

class IntegerAverageAggregator extends AbstractIntegerAggregator {
    private IntegerSumAggregator sum = new IntegerSumAggregator();
    private IntegerCountAggregator count = new IntegerCountAggregator();

    @Override
    public void apply(Field group, Integer value) {
        sum.apply(group, value);
        count.apply(group, value);
    }

    @Override
    public Map<Field, Integer> result() {
        Map<Field, Integer> sumResult = sum.result();
        Map<Field, Integer> countResult = count.result();

        sumResult.forEach((group, sum) -> {
            groups.put(group, sum / countResult.get(group));
        });

        return groups;
    }
}