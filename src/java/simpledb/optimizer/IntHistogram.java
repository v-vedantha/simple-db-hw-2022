package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    // Store the bucket number
    private int buckets;
    // Store the min value
    private int min;
    // Store the max value
    private int max;
    private int total;
    // Store the bucket counts
    private int[] bucketCounts;
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        if (buckets > max - min) {
            this.buckets = max - min ;
        }
        else{
            this.buckets = buckets;
        }
        this.min = min;
        this.max = max;
        bucketCounts = new int[this.buckets];
        total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */

    private int getBucketIndex(int v) {
        if (v == max) {
            return buckets - 1;
        }
        double width = (max - min) / buckets;
        int index =  (int) ((v - min)  / width);
        return index;
    }
    public void addValue(int v) {
        // some code goes here
        total++;
        int bucketIndex = getBucketIndex(v);
        bucketCounts[bucketIndex]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateEqual(int v) {
        // some code goes here
        if (v < min || v > max) {
            return 0.0;
        }
        int bucketIndex = getBucketIndex(v);
        double width = (max - min) / (double) buckets;
        double op =  (bucketCounts[bucketIndex] / width)/total;
        return op;
    }

    public double estimateLessThan(int v)
    {
        // some code goes here
        if (v <= min) {
            return 0.0;
        }
        if (v > max) {
            return 1.0;
        }
        int bucketIndex = getBucketIndex(v);
        double fraction = (v - min) / (double) (max - min) * buckets - bucketIndex;
        double lessThan = 0.0;
        for (int i = 0; i < bucketIndex; i++) {
            lessThan += bucketCounts[i];
        }
        lessThan += fraction * bucketCounts[bucketIndex];
        return lessThan / total;
    }

    public double estimateLessThanEquals(int v)
    {
        // some code goes here
        if (v < min) {
            return 0.0;
        }
        if (v >= max) {
            return 1.0;
        }
        int bucketIndex = getBucketIndex(v);
        double fraction = (v - min) / (double) (max - min) * buckets - bucketIndex;
        double lessThan = 0.0;
        for (int i = 0; i < bucketIndex; i++) {
            lessThan += bucketCounts[i];
        }
        lessThan += (fraction + 1) * bucketCounts[bucketIndex];
        return lessThan / total;
    }

    public double estimateGreaterThan(int v)
    {
        // some code goes here
        if (v < min) {
            return 1.0;
        }
        if (v >= max) {
            return 0.0;
        }
        int bucketIndex = getBucketIndex(v);
        double fraction = (v - min) / (double) (max - min) * buckets - bucketIndex;
        double greaterThan = 0.0;
        for (int i = bucketIndex + 1; i < buckets; i++) {
            greaterThan += bucketCounts[i];
        }
        greaterThan += (1 - fraction) * bucketCounts[bucketIndex];
        return greaterThan / total;
    }

    public double estimateGreaterThanEquals(int v)
    {
        // some code goes here
        if (v <= min) {
            return 1.0;
        }
        if (v > max) {
            return 0.0;
        }
        int bucketIndex = getBucketIndex(v);
        double fraction = (v - min) / (double) (max - min) * buckets - bucketIndex;
        double greaterThan = 0.0;
        for (int i = bucketIndex + 1; i < buckets; i++) {
            greaterThan += bucketCounts[i];
        }
        greaterThan += (1 - fraction + 1) * bucketCounts[bucketIndex];
        return greaterThan / total;
    }

    public double estimateNotEquals(int v)
    {
        // some code goes here
        if (v < min || v > max) {
            return 1.0;
        }
        int bucketIndex = getBucketIndex(v);
        return 1.0-((bucketCounts[bucketIndex] / ((double) (max - min) / buckets))/total);
    }
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        double selectivity = 0;
        switch (op) {
            case EQUALS:
                selectivity = estimateEqual(v);
                break;
            case NOT_EQUALS:
                selectivity = estimateNotEquals(v);
                break;
            case GREATER_THAN:
                selectivity = estimateGreaterThan(v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateGreaterThanEquals(v);
                break;
            case LESS_THAN:
                selectivity = estimateLessThan(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateLessThanEquals(v);
                break;
        }
        return selectivity;

    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return null;
    }
}
