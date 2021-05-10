package simpledb;


import static java.lang.Math.min;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int minInt, maxInt, width, bucketNum, numAll;
    private int[] bucketCnt; // only the last bucket may be less than width
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
    	// some code goes here
        this.numAll = 0;
        this.minInt = min;
        this.maxInt = max;
        this.bucketNum = buckets;
        this.bucketCnt = new int[buckets];
        this.width = (max - min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int ind = min((v - this.minInt) / this.width, this.bucketNum - 1);
        ++bucketCnt[ind];
        ++numAll;
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
    	// some code goes here
        double res = 0.;
        if (v < minInt || v > maxInt) {
            switch (op) {
                case EQUALS:
                    return 0.0;
                case NOT_EQUALS:
                    return 1.0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return (v < minInt) ? 0.0 : 1.0;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return (v > maxInt) ? 0.0 : 1.0;
            }
        }

        int ind = min((v - minInt) / width, bucketNum - 1);
        switch (op) {
            case EQUALS:
                break;

            case GREATER_THAN:
                break;
            case LESS_THAN:
                break;
            case LESS_THAN_OR_EQ :
                break;
            case GREATER_THAN_OR_EQ:
                break;
            case LIKE:
                break;
            case NOT_EQUALS:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + op);
        }
        return -1.0;
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
        // some code goes here
        return null;
    }
}
