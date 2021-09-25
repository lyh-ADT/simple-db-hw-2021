package simpledb.optimizer;

import java.util.HashMap;
import java.util.Map.Entry;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final HashMap<Integer, Integer> buckets;
    private final int minValue;
    private final int maxValue;
    private final int bucketSize;
    private int totalCount;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new HashMap<Integer, Integer>(buckets);
        this.minValue = min;
        this.maxValue = max;
        this.bucketSize = (int)Math.ceil((max - min) / (double)buckets);
        this.totalCount = 0;
    }

    private int calcValuePos(int value) {
        return (value - minValue) / bucketSize;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        buckets.compute(calcValuePos(v), (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = 0;
            }
            return oldValue + 1;
        });
        this.totalCount++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        ThreadLocal<Integer> count = new ThreadLocal<Integer>();
        count.set(0);

        buckets.keySet().stream()
            .filter(getFiter(op, v))
            .forEach(k -> {
                count.set(count.get() + buckets.get(k));
            });
        
        Integer remainTupleCount = count.get();
        if (op == Predicate.Op.EQUALS) {
            remainTupleCount /= bucketSize;
        }
        return remainTupleCount / (double)totalCount;
    }

    private java.util.function.Predicate<Integer> getFiter(Predicate.Op op, int v) {
        int key = calcValuePos(v);
        switch(op) {
            case EQUALS:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t.equals(key);
                    }
                    
                };
            case GREATER_THAN:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t > key;
                    }
                    
                };
            case GREATER_THAN_OR_EQ:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t >= key;
                    }
                    
                };
            case LESS_THAN:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t < key;
                    }
                    
                };
            case LESS_THAN_OR_EQ:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t <= key;
                    }
                    
                };
            case NOT_EQUALS:
                return new java.util.function.Predicate<Integer>(){

                    @Override
                    public boolean test(Integer t) {
                        return t != key;
                    }
                    
                };
            case LIKE:
            default:
                throw new UnsupportedOperationException();
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("IntHistorgram{");
        for (Entry<Integer, Integer> entry : buckets.entrySet()) {
            sb.append(String.format("%s: %s\n", entry.getKey(), entry.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }
}
