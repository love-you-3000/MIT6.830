package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // 桶的个数
    // 该列最小值
    private int min;

    // 该列最大值
    private int max;

    // 每个桶的宽度,
    private double width;

    // 该列全部元素的数量
    private int nTuples;
    // 存放不同的单桶
    int[] buckets;

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
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        // 桶的数量
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.nTuples = 0;
        //一个桶的数据范围
        this.width = Math.max(1, (max - min + 1.0) / buckets);
    }


    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= min && v <= max && getIndex(v) != -1) {
            buckets[getIndex(v)]++;
            nTuples++;
        }
    }

    public int getIndex(int v) {
        int index = (int) ((v - min) / width);
        if (index < 0 || index >= buckets.length) {
            return -1;
        }
        return index;
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
//    public double estimateSelectivity(Predicate.Op op, int v) {
//        // some code goes here
//        if (op == Predicate.Op.EQUALS) return equalsRatio(v);
//        else if (op == Predicate.Op.GREATER_THAN) return lessThanRatio(v);
//        else if (op == Predicate.Op.LESS_THAN) return lessThanRatio(v);
//        else if (op == Predicate.Op.NOT_EQUALS) return 1 - equalsRatio(v);
//        else if (op == Predicate.Op.LESS_THAN_OR_EQ) return equalsRatio(v) + lessThanRatio(v);
//        else if (op == Predicate.Op.GREATER_THAN_OR_EQ) return equalsRatio(v) + greaterThanRatio(v);
//        return 0.0;
//    }
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        // case: 2、3、4、5； 6，7，8，9； v = 7
        switch (op) {
            // 8，9
            case GREATER_THAN:
                if (v > max) {
                    return 0.0;
                } else if (v <= min) {
                    return 1.0;
                } else {
                    int index = getIndex(v);
                    double tuples = 0;
                    for (int i = index + 1; i < buckets.length; i++) {
                        tuples += buckets[i];
                    }
                    // 2 * 4 + 2 - 1 -7
                    tuples += (min + (getIndex(v) + 1) * width - 1 - v) * (1.0 * buckets[index] / width);
                    return tuples / nTuples;
                }
            case LESS_THAN:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                        estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            default:
                throw new UnsupportedOperationException("Op is illegal");
        }
    }

//
//    private double equalsRatio(int v) {
//        // 首先获取该值对应的桶
//        int target = getIndex(v);
//        // 该桶中对应tuple的数量
//        int count = buckets[target];
//        return (count * 1.0 / nTuples) / width;
//    }
//
//    private double greaterThanRatio(int v) {
//        // 首先获取该值对应的桶
//        int target = getIndex(v);
//        // 该桶中对应tuple的数量
//        int count = buckets[target];
//        // 首先算该值对应的桶中大于v的值的占比
//        double selectivity = (double) (((curr.right - v) / width) * curr.count) / nTuples;
//        // 然后把剩余的桶的占比累加
//        for (int i = target + 1; i < bucketsNum; i++) {
//            selectivity += (double) buckets[i]/ nTuples;
//        }
//        return selectivity;
//    }
//
//    private double lessThanRatio(int v) {
//        // 首先获取该值对应的桶
//        int target = getWhichBucket(v);
//        // 该桶中对应tuple的数量
//        Bucket curr = buckets[target];
//        double selectivity = (double) (((v - curr.left) / width) * curr.count) / nTuples;
//        for (int i = target - 1; i >= 0; i--) {
//            selectivity += (double) buckets[i].count / nTuples;
//        }
//        return selectivity;
//
//    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        double sum = 0;
        for (int bucket : buckets) {
            sum += (1.0 * bucket / nTuples);
        }
        return sum / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "buckets = " + Arrays.toString(buckets) +
                ", min = " + min +
                ", max =" + max +
                ", width =" + width +
                "}";
    }

}
