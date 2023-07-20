package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;

/**
 * @className: JoinTest
 * @author: 朱江
 * @description:
 * @date: 2023/7/19
 **/
public class JoinTest {

    private static String tupleDesc = "f0  f1  f2";

    /**
     * select * from t1,t2 where t1.f0 > 1 and t1.f1 = t2.f1 ;
     */
    public static void main(String[] args) throws TransactionAbortedException, DbException {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"f0", "f1", "f2"};
        TupleDesc td = new TupleDesc(types, names);
        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        System.out.println(tupleDesc);
        ss1.open();
        while (ss1.hasNext()) System.out.println(ss1.next());
        System.out.println("--------");
        System.out.println(tupleDesc);
        ss2.open();
        while (ss2.hasNext()) System.out.println(ss2.next());
        System.out.println("--------");
        System.out.println("Execute: select * from t1,t2 where t1.f0 > 1 and t1.f1 = t2.f1 ");
        System.out.println("result:");

//         create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);
        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
