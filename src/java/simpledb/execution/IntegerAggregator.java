package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUP = new IntField(-1);

    // todo 这块理解了好久，翻译成sql就是 select gbfield, what(afield) from table group by gbfield;
    private int gbfield; // 用于分组的字段索引，从0开始，如果是-1就是不分组

    private Type gbfieldtype; // 分组字段的类型，如果没有分组字段，则为null

    private int afield; // 聚合哪个字段

    private Op what; // 执行那种聚合

    /**
     * 存放结果
     */
    private TupleDesc tupleDesc;
    private Map<Field, Tuple> aggregate;
    /**
     * 用于非分组情况下的聚合操作
     */
    private int counts;
    private int summary;
    /**
     * 用于分组情况下的聚合操作
     */
    private Map<Field, Integer> countsMap;
    private Map<Field, Integer> sumMap;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        //分组字段
        this.gbfield = gbfield;
        //分组字段类型
        this.gbfieldtype = gbfieldtype;
        //聚合得到的结果,在聚合返回结果行中的字段下标
        this.afield = afield;
        //进行什么样的聚合操作
        this.what = what;
        //存放聚合结果
        this.aggregate = new ConcurrentHashMap<>();
        // 非分组聚合
        if (gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
            Tuple tuple = new Tuple(tupleDesc);
            //占位符
            this.aggregate.put(NO_GROUP, tuple);
        } else {
            // 分组聚合,那么返回的聚合结果行由分组字段和该分组字段的聚合结果值组成
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
        // 如果聚合操作是AVG,那么需要初始化count和summary变量,用于存放AVG聚合中间计算状态
        if (gbfield == NO_GROUPING && what.equals(Op.AVG)) {
            this.counts = 0;
            this.summary = 0;
        } else if (gbfield != NO_GROUPING && what.equals(Op.AVG)) {
            this.countsMap = new ConcurrentHashMap<>();
            this.sumMap = new ConcurrentHashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // 从传递给聚合器的行记录中取出聚合字段的值
        IntField operationField = (IntField) tup.getField(this.afield);
        if (operationField == null) return;
        // 非分组聚合
        if (gbfield == NO_GROUPING) {
            Tuple tuple = aggregate.get(NO_GROUP);
            IntField field = (IntField) tuple.getField(0);
            // 说明是进行聚合的第一行记录
            if (field == null) {
                // 如果聚合是统计个数操作
                if (what.equals(Op.COUNT)) {
                    tuple.setField(0, new IntField(1));
                } else if (what.equals(Op.AVG)) {
                    // 如果聚合是求平均值操作
                    // 统计参与聚合的记录个数
                    counts++;
                    // 累加每个值
                    summary = operationField.getValue();
                    // 如果参与聚合的行只存在一个,那么平均值就是当前行的值
                    tuple.setField(0, operationField);
                } else {
                    // 其他的情况: MIN,MAX,SUM在参与聚合的行只存在一个时,聚合结果就是当前行的值
                    // 所以这里可以统一处理
                    tuple.setField(0, operationField);
                }
                return;
            }
            // 判断是哪种类型的聚合
            // 非第一行记录
            switch (what) {
                case MIN:
                    if (operationField.compare(Predicate.Op.LESS_THAN, field)) {
                        tuple.setField(0, operationField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case MAX:
                    if (operationField.compare(Predicate.Op.GREATER_THAN, field)) {
                        tuple.setField(0, operationField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case COUNT:
                    tuple.setField(0, new IntField(field.getValue() + 1));
                    aggregate.put(NO_GROUP, tuple);
                case SUM:
                    tuple.setField(0, new IntField(field.getValue() + operationField.getValue()));
                    aggregate.put(NO_GROUP, tuple);
                    return;
                case AVG:
                    // 求平均值,每次往整数聚合器塞入一条记录时,都会将记录数和总和累加
                    counts++;
                    summary += operationField.getValue();
                    IntField avg = new IntField(summary / counts);
                    tuple.setField(0, avg);
                    aggregate.put(NO_GROUP, tuple);
                    return;
                default:
                    return;
            }
        }
        // 分组聚合
        else {
            // 获取分组的字段
            Field groupByField = tup.getField(gbfield);
            // 如果聚合结果中还不包括当前字段值,说明当前字段是第一次出现
            // 例如: group by age --> <age=18,count=20> ,如果此次获取的age=20,那么就是第一次出现的分组值
            if (!aggregate.containsKey(groupByField)) {
                Tuple value = new Tuple(this.tupleDesc);
                value.setField(0, groupByField);
                if (what.equals(Op.COUNT)) {
                    value.setField(1, new IntField(1));
                } else if (what.equals(Op.AVG)) {
                    countsMap.put(groupByField, countsMap.getOrDefault(groupByField, 0) + 1);
                    sumMap.put(groupByField, sumMap.getOrDefault(groupByField, 0) + operationField.getValue());
                    value.setField(1, operationField);
                } else {
                    // 其他的情况: MIN,MAX,SUM在参与聚合的行只存在一个时,结果假设当前行的值
                    // 所以这里可以统一处理
                    value.setField(1, operationField);
                }
                aggregate.put(groupByField, value);
                return;
            }
            // 当前字段不是第一次出现的分组值
            Tuple tuple = aggregate.get(groupByField);
            // 获取本阶段的聚合结果
            IntField field = (IntField) tuple.getField(1);
            switch (what) {
                case MIN:
                    if (operationField.compare(Predicate.Op.LESS_THAN, field)) {
                        tuple.setField(1, operationField);
                        aggregate.put(groupByField, tuple);
                    }
                    return;
                case MAX:
                    if (operationField.compare(Predicate.Op.GREATER_THAN, field)) {
                        tuple.setField(1, operationField);
                        aggregate.put(groupByField, tuple);
                    }
                    return;
                case COUNT:
                    IntField count = new IntField(field.getValue() + 1);
                    tuple.setField(1, count);
                    aggregate.put(groupByField, tuple);
                    return;
                case SUM:
                    IntField sum = new IntField(field.getValue() + operationField.getValue());
                    tuple.setField(1, sum);
                    aggregate.put(groupByField, tuple);
                    return;
                case AVG:
                    countsMap.put(groupByField, countsMap.getOrDefault(groupByField, 0) + 1);
                    sumMap.put(groupByField, sumMap.getOrDefault(groupByField, 0) + operationField.getValue());
                    IntField avg = new IntField(sumMap.get(groupByField) / countsMap.get(groupByField));
                    tuple.setField(1, avg);
                    aggregate.put(groupByField, tuple);
                    return;
                default:
                    return;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntOpIterator(this);
    }

    public class IntOpIterator implements OpIterator {
        private Iterator<Tuple> iterator;
        private IntegerAggregator aggregator;

        public IntOpIterator(IntegerAggregator aggregator) {
            this.aggregator = aggregator;
            this.iterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }


}
