package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */

// 对外暴露该类来执行sql
public class Aggregate extends Operator {
    private static final long serialVersionUID = 1L;

    private OpIterator child; // 用于不断提供Tuples

    private int afield; // 需要聚合的字段

    private int gfield; // 需要group by的列

    private Aggregator.Op aop; // 聚合操作

    private Aggregator aggregator; // 具体的聚合运算类型
    private OpIterator it; // // 聚合结果迭代器，存着一个个的tuple
    private TupleDesc td; // 聚合结果的表描述


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        // 聚合类型,如果是-1,,说明不分组聚合
        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
        // 选用合适的聚合器
        if(this.child.getTupleDesc().getFieldType(this.afield) == (Type.STRING_TYPE)){
            this.aggregator = new StringAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }else{
            this.aggregator = new IntegerAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }
        // 获取聚合器的迭代器
        this.it = this.aggregator.iterator();
        // 构造聚合结果的表头信息
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        // 如果分组了，第一列表头是gfield
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        // 注意，如果上面判断分组的条件没有执行，也就是乜有分组，那么types和names列表的长度 应该是1，如果执行了就是2
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));

        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        this.td = new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return  this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        // some code goes here
        if(this.gfield == -1)
            return this.td.getFieldName(0);
        else
            return this.td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here

        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        this.child.open();
        while (this.child.hasNext())
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        this.it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // some code goes here
        while (this.it.hasNext())
            return this.it.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
//        List<Type> types = new ArrayList<>();
//        List<String> names = new ArrayList<>();
//        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
//        // group field
//        if (gfieldtype != null) {
//            types.add(gfieldtype);
//            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
//        }
//        types.add(this.child.getTupleDesc().getFieldType(this.afield));
//        names.add(this.child.getTupleDesc().getFieldName(this.afield));
//        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
//            types.add(Type.INT_TYPE);
//            names.add("COUNT");
//        }
//        this.td = new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }
}
