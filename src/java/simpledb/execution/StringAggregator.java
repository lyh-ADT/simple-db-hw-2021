package simpledb.execution;

import java.io.DataOutputStream;
import java.io.IOException;
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
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new Field() {
        @Override
        public void serialize(DataOutputStream dos) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean compare(simpledb.execution.Predicate.Op op, Field value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getType() {
            throw new UnsupportedOperationException();
        }
    };

    private int groupByFieldIndex;
    private Type groupByFieldType;
    private int aggregateFiledIndex;
    private Op operation;
    private HashMap<Field, Integer> groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFiledIndex = afield;
        this.operation = what;
        if (operation != Op.COUNT) {
            throw new UnsupportedOperationException();
        }
        this.groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gField = groupByFieldIndex == NO_GROUPING ? NO_GROUP_FIELD : tup.getField(groupByFieldIndex);
        groups.compute(gField, (g, oldValue) -> {
            if (oldValue == null) {
                oldValue = 0;
            }
            return oldValue + 1;
        });
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
        return new Iterator(groups);
    }

    class Iterator implements OpIterator {
        private Map<Field, Integer> result;
        private java.util.Iterator<Entry<Field, Integer>> iterator;
        private TupleDesc tupleDesc;

        public Iterator(Map<Field, Integer> result) {
            this.result = result;
            if (result.containsKey(NO_GROUP_FIELD)) {
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
            if (NO_GROUP_FIELD == entry.getKey()) {
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
            iterator = null;
        }
    }


}
