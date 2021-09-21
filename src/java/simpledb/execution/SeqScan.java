package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleDesc.TDItem;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transactionId;
    private int tableId;
    private String tableAlias;
    private final DbFile dbFile;
    private DbFileIterator tupleIterator;
    private TupleDesc tupleDesc;

    /**
     * Creates a sequential scan over the specified table as a part of the specified
     * transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the
     *                   returned tupleDesc should have fields with name
     *                   tableAlias.fieldName (note: this class is not responsible
     *                   for handling a case where tableAlias or fieldName are null.
     *                   It shouldn't crash if they are, but the resulting name can
     *                   be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.transactionId = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);

        this.buildAliasTupleDesc();
    }

    /**
     * @return return the table name of the table the operator scans. This should be
     *         the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the
     *                   returned tupleDesc should have fields with name
     *                   tableAlias.fieldName (note: this class is not responsible
     *                   for handling a case where tableAlias or fieldName are null.
     *                   It shouldn't crash if they are, but the resulting name can
     *                   be null.fieldName, tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.tupleIterator = dbFile.iterator(transactionId);
        this.tupleIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile, prefixed
     * with the tableAlias string from the constructor. This prefix becomes useful
     * when joining tables containing a field(s) with the same name. The alias and
     * name should be separated with a "." character (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile, prefixed
     *         with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (tupleIterator == null) {
            throw new NoSuchElementException("should open this opIterator first");
        }
        return tupleIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        if (tupleIterator == null) {
            throw new NoSuchElementException("should open this opIterator first");
        }
        return tupleIterator.next();
    }

    public void close() {
        if (tupleIterator == null) {
            // closed already. do nothing
            return;
        }
        tupleIterator.close();
        tupleIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        open();
    }

    /**
     * build TupleDesc with aliasName as {@link SeqScan#getTupleDesc()} wanted
     */
    private void buildAliasTupleDesc() {
        Iterator<TDItem> iterator = dbFile.getTupleDesc().iterator();
        Type[] types = new Type[dbFile.getTupleDesc().numFields()];
        String[] names = new String[dbFile.getTupleDesc().numFields()];
        int i = 0;
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            types[i] = item.fieldType;
            names[i] = String.format("%s.%s", this.tableAlias, item.fieldName);
            i++;
        }
        this.tupleDesc = new TupleDesc(types, names);

    }
}
