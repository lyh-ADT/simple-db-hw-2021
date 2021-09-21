package simpledb.execution;

import simpledb.execution.Predicate.Op;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int field1Index;
    private final int field2Index;
    private final Op operation;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     * 
     *               field2 op field1
     * 
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1Index = field1;
        this.field2Index = field2;
        this.operation = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be made
     * through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1Index).compare(operation, t2.getField(field2Index));
    }

    public int getField1() {
        return field1Index;
    }

    public int getField2() {
        return field2Index;
    }

    public Predicate.Op getOperator() {
        return operation;
    }
}
