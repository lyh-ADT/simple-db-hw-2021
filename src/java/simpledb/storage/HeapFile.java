package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final Map<PageId, HeapPage> newPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.newPages = new HashMap<>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you
     * will need to generate this tableid somewhere to ensure that each HeapFile has
     * a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying
     * the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // 检查是否是新建的page
        if (this.newPages.containsKey(pid)) {
            return this.newPages.get(pid);
        }
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        if (offset > file.length()) {
            throw new IllegalArgumentException();
        }
        try (RandomAccessFile rf = new RandomAccessFile(file, "r");) {
            byte[] data = new byte[BufferPool.getPageSize()];
            rf.seek(offset);
            rf.readFully(data, 0, data.length);
            return new HeapPage(new HeapPageId(this.getId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length() / BufferPool.getPageSize() + this.newPages.size();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 遍历每个page找出有空位的进行添加
        for(int pageNo=0; pageNo < this.numPages(); pageNo++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), pageNo), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return Collections.singletonList(page);
            }
        }

        // 当前文件已满，要创建新的页
        HeapPage page = this.createNewPage();
        page.insertTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int pid;
            private HeapPage page;
            private Iterator<Tuple> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pid = 0;
                HeapPageId hPid = new HeapPageId(HeapFile.this.getId(), pid);
                page = (HeapPage) Database.getBufferPool().getPage(tid, hPid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) {
                    // did not open
                    return false;
                }
                if (iterator.hasNext()) {
                    return true;
                }
                if (pid >= HeapFile.this.numPages() - 1) {
                    return false;
                }
                openNextPage();
                return hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iterator == null) {
                    throw new NoSuchElementException("should open this iterator first");
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                iterator = null;
            }

            private void openNextPage() throws TransactionAbortedException, DbException {
                HeapPageId hPid = new HeapPageId(HeapFile.this.getId(), ++pid);
                page = (HeapPage) Database.getBufferPool().getPage(tid, hPid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }
        };
    }

    private HeapPage createNewPage() throws IOException {
        HeapPageId pageId = new HeapPageId(this.getId(), numPages());
        HeapPage page = new HeapPage(
            pageId,
            HeapPage.createEmptyPageData()
        );

        this.newPages.put(pageId, page);
        return page;
    }
}
