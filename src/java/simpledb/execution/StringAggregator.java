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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final IntField NO_GROUP = new IntField(-1);
    /**
     * 用于分组
     */
    private int gbfield;
    private Type gbfieldtype;
    /**
     * 用于聚合
     */
    private int afield;
    private Op what;

    /**
     * 存放结果-- 分组聚合返回的是多组键值对,分别代表分组字段不同值对应的聚合结果
     * 非分组聚合只会返回一个聚合结果,这里为了统一化处理,采用NO_GROUP做标记,进行区分
     */
    private Map<Field, Tuple> tupleMap;
    private TupleDesc desc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        //字符串只支持COUNT聚合操作
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.tupleMap = new ConcurrentHashMap<>();
        //非分组聚合返回的结果采用占位符进行统一适配
        if (gbfield == NO_GROUPING) {
            this.desc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
            Tuple tuple = new Tuple(desc);
            tuple.setField(0, new IntField(0));
            this.tupleMap.put(NO_GROUP, tuple);
        } else {
            //分组聚合返回结果Schema由两个字段组成: 分组字段和聚合结果
            this.desc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbfield == NO_GROUPING) {
            Tuple tuple = this.tupleMap.get(NO_GROUP);
            IntField field = (IntField) tuple.getField(0);
            tuple.setField(0, new IntField(field.getValue() + 1));
            tupleMap.put(NO_GROUP, tuple);
        } else {
            Field field = tup.getField(gbfield);
            if (!tupleMap.containsKey(field)) {
                Tuple tuple = new Tuple(this.desc);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(1));
                tupleMap.put(field, tuple);
            } else {
                Tuple tuple = tupleMap.get(field);
                IntField intField = (IntField) tuple.getField(1);
                tuple.setField(1, new IntField(intField.getValue() + 1));
                tupleMap.put(field, tuple);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringIterator(this);
    }

    public class StringIterator implements OpIterator {
        private StringAggregator aggregator;
        private Iterator<Tuple> iterator;

        public StringIterator(StringAggregator aggregator) {
            this.aggregator = aggregator;
            this.iterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = aggregator.tupleMap.values().iterator();
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
            iterator = aggregator.tupleMap.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.desc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}
