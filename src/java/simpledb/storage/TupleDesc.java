package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
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

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
            result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
            return result;
        }

        /**
         * 只比较type
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TDItem other = (TDItem) obj;
            if (fieldType != other.fieldType)
                return false;
            return true;
        }

    }

    private ArrayList<TDItem> fields;

    /**
     * @return An iterator which iterates over all the field TDItems that are
     *         included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return this.fields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified
     * types, with associated named fields.
     * 
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("类型和名字的数量不相等");
        }
        this.fields = new ArrayList<TDItem>();
        for (int i = 0; i < fieldAr.length; i++) {
            fields.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of
     * the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return getField(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return getField(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).fieldName != null && fields.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
     *         that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalBytes = 0;
        for (TDItem tdItem : fields) {
            totalBytes += tdItem.fieldType.getLen();
        }
        return totalBytes;
    }

    private TDItem getField(int index) throws NoSuchElementException {
        if (index < 0 || index >= fields.size()) {
            throw new NoSuchElementException();
        }
        return fields.get(index);
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
        final int totalSize = td1.fields.size() + td2.fields.size();
        Type[] types = new Type[totalSize];
        String[] names = new String[totalSize];

        int index = 0;
        for (TDItem field : td1.fields) {
            types[index] = field.fieldType;
            names[index] = field.fieldName;
            index++;
        }
        for (TDItem field : td2.fields) {
            types[index] = field.fieldType;
            names[index] = field.fieldName;
            index++;
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items and if
     * the i-th type in this TupleDesc is equal to the i-th type in o for every i.
     * 
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc t = (TupleDesc) o;
        return fields.equals(t.fields);
    }

    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
     * exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        return String.format("TupleDesc%s", fields);
    }
}
