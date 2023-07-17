package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 * 一个tuple就是表中一行数据，TupleDesc就是表中每列字段的描述
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */

    private ArrayList<TDItem> tdItems;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    public void setFieldAr(String[] fieldAr) {
        this.fieldAr = fieldAr;
    }

    private static final long serialVersionUID = 1L;

    private static final String anonymousField = "unnamed";
    private Type[] typeAr;

    private String[] fieldAr;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        // 判断字段类型长度和字段名称长度是否一致
        if (typeAr.length != fieldAr.length) throw new RuntimeException();
        int n = typeAr.length;
        if (n == 0) throw new RuntimeException();
        this.typeAr = typeAr;
        this.fieldAr = fieldAr;
        saveTdItem();
    }

    public Type[] getTypeAr() {
        return typeAr;
    }

    public void setTypeAr(Type[] typeAr) {
        this.typeAr = typeAr;
    }

    public String[] getFieldAr() {
        return fieldAr;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.typeAr = typeAr;
        this.fieldAr = new String[typeAr.length];
        Arrays.fill(fieldAr, anonymousField);
        saveTdItem();
        // some code goes here
    }

    public void saveTdItem() {
        tdItems = new ArrayList<>();
        for (int i = 0; i < this.fieldAr.length; i++)
            tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {
            if (tdItems.get(i).fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem tdItem : tdItems) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int n = td1.numFields() + td2.numFields();
        Type[] types = new Type[n];
        String[] strings = new String[n];
        int ind = 0;
        Iterator<TDItem> iterator1 = td1.iterator();
        Iterator<TDItem> iterator2 = td2.iterator();
        while (iterator1.hasNext()) {
            TDItem next = iterator1.next();
            types[ind] = next.fieldType;
            strings[ind++] = next.fieldName;

        }
        while (iterator2.hasNext()) {
            TDItem next = iterator2.next();
            types[ind] = next.fieldType;
            strings[ind++] = next.fieldName;
        }
        return new TupleDesc(types, strings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            TupleDesc comp = (TupleDesc) o;
            if (comp.numFields() != this.numFields()) return false;
            for (int i = 0; i < numFields(); i++) {
                if (!this.getFieldType(i).equals(comp.getFieldType(i))) return false;
            }
            return true;
        }
        // some code goes here
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < tdItems.size(); i++) {
            msg.append(tdItems.get(i).fieldType).append("(").append(tdItems.get(i).fieldName).append(")");
            if (i < tdItems.size() - 1) msg.append(",");
        }
        return msg.toString();
    }
}
