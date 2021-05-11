package simpledb;


import static java.lang.Math.max;
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
        this.bucketNum = min(buckets, max - min + 1);
        this.bucketCnt = new int[buckets];
        this.width = ((max - min + 1) / this.bucketNum);
    }

    private int getInd(int v){
        return  min((v - this.minInt) / this.width, this.bucketNum - 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        ++bucketCnt[getInd(v)];
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

        int ind = getInd(v);
        int height = bucketCnt[ind];
        double res = 0.;
        int left = max(minInt + ind * width, minInt);
        int right = min(minInt + ind * width + width - 1, maxInt);
        switch (op) {
            case EQUALS:
                return (height * 1.) / (right - left + 1) / numAll;
            case GREATER_THAN:
                res = (1. * (right - v) / width) * (1. * height / numAll);
                for (ind = ind + 1; ind < bucketNum; ++ind)
                    res += 1. * bucketCnt[ind] / numAll;
                return res;
            case LESS_THAN:
                res = (1. * (v - left) / width) * (1. * height / numAll);
                for (ind = ind - 1; ind >= 0; --ind)
                    res += 1. * bucketCnt[ind] / numAll;
                return res;
            case LESS_THAN_OR_EQ :
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new IllegalStateException("Unexpected value: " + op);
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
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("IntHistogram: \n");
        stringBuilder.append(String.format("Min: {} \t Max: {} \t Buckets: {}\n", minInt, maxInt, bucketNum));
        for (int i = 0; i < bucketNum; i++)
        {
            stringBuilder.append(bucketCnt[i]).append("\t");
        }
        return stringBuilder.toString();
    }
}
