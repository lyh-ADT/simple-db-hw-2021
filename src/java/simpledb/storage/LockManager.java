package simpledb.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import simpledb.transaction.TransactionId;

/**
 * 
 * Map<TransactionId, Set<PageLock>> 一个事务可以持有多个页的锁
 * Map<PageId, Set<TransactionLock>> 一个页的读锁可以被多个事务持有
 * 
 * getPage -> pid -> lock
 * commit -> tid -> pid
 * 
 */
public class LockManager {
    private final Map<PageId, Lock> locks = new HashMap<>();

    /**
     * 通过tid获取对应pid的读锁，一直阻塞直到获取到为止
     */
    public synchronized void acquireReadLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        lock.readLock(tid);
    }

    /**
     * 通过tid获取对应pid的写锁，一直阻塞直到获取到为止
     */
    public synchronized void acquireWriteLock(PageId pid, TransactionId tid) {
        Lock lock = getLock(pid);
        lock.writeLock(tid);
    }

    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        getLock(pid).releaseLock(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        return getLock(p).owners.contains(tid);
    }

    private Lock getLock(PageId pid) {
        Lock lock = locks.get(pid);
        if (lock == null) {
            lock = new Lock();
            locks.put(pid, lock);
        }
        return lock;
    }

    class Lock {
        private Set<TransactionId> owners = new HashSet<>();
        private boolean lockingRead = true;

        public void readLock(TransactionId tid) {
            if (owners.isEmpty()) {
                lockingRead = true;
            }
            if (lockingRead) {
                owners.add(tid);
                return;
            }
            // 有人持有writeLock
            // 如果tid已经持有写锁，那么默认获取读锁
            if (owners.contains(tid)) {
                return;
            }
            while (!owners.isEmpty()) {
            }
            readLock(tid);
        }

        public void writeLock(TransactionId tid) {
            if (owners.isEmpty()) {
                lockingRead = false;
            }
            if (!lockingRead) {
                owners.add(tid);
                return;
            }
            // 有人持有readLock
            // 如果持有者是同一个可以升级
            if (owners.size() == 1 && owners.contains(tid)) {
                lockingRead = true;
                return;
            }
            while (!owners.isEmpty()) {
            }
            writeLock(tid);
        }

        public void releaseLock(TransactionId tid) {
            if (!owners.contains(tid)) {
                throw new IllegalStateException(String.format("%s事务", tid));
            }
            owners.remove(tid);
        }
    }
}
