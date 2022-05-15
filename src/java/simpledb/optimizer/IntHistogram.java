package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;

    private int cnt;

    private int width;

    private int[] bucket;

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
        this.min = min;
        this.cnt = 0;
        this.width = (max - min + 1) / buckets;
        this.bucket = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = (v - this.min) / this.width;
        bucket[index]++;
        this.cnt++;
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
        double result;
        int index = (v - this.min) / this.width;
        switch(op) {
            case EQUALS: {
                result = bucket[index]*1.0 / this.width / this.cnt;
                break;
            }
            case LIKE: {
                result = bucket[index]*1.0 / this.width / this.cnt;
                break;
            }
            case GREATER_THAN: {
                result = bucket[index]*1.0*((index+1)*this.width - v) / this.width;
                for(int i = index + 1; i < bucket.length; i++) {
                    result += bucket[i];
                }
                result /= this.cnt;
                break;
            }
            case LESS_THAN: {
                result = bucket[index]*1.0*(v - index*this.width - 1) / this.width;
                for(int i = index - 1; i >= 0; i--) {
                    result +=bucket[i];
                }
                result /= this.cnt;
                break;
            }
            case GREATER_THAN_OR_EQ: {
                result = bucket[index]*1.0*((index+1)*this.width - v) / this.width
                        + bucket[index]*1.0 / this.width;
                for(int i = index + 1; i < bucket.length; i++) {
                    result += bucket[i];
                }
                result /= this.cnt;
                break;
            }
            case LESS_THAN_OR_EQ: {
                result = bucket[index]*1.0*(v - index*this.width - 1) / this.width
                        + bucket[index]*1.0 / this.width;
                for(int i = index - 1; i >= 0; i--) {
                    result +=bucket[i];
                }
                result /= this.cnt;
                break;
            }
            case NOT_EQUALS: {
                result = bucket[index]*1.0 / this.width / this.cnt;
                result = 1 - result;
                break;
            }
            default: result = -1.0;
        }
        return result;
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
