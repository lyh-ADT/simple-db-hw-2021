package simpledb.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    private final Map<PageId, Lock> locks = new HashMap<>();
    private final DeadLockDetector detector = new DeadLockDetector();

    /**
     * 通过tid获取对应pid的读锁，一直阻塞直到获取到为止
     * @throws TransactionAbortedException
     */
    public void acquireReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        Lock lock = getLock(pid);
        if (detector.waitFor(tid, pid)) {
            throw new TransactionAbortedException();
        }
        lock.readLock(tid);
        detector.lockGranted(tid, pid);
    }

    /**
     * 通过tid获取对应pid的写锁，一直阻塞直到获取到为止
     * @throws TransactionAbortedException
     */
    public void acquireWriteLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        Lock lock = getLock(pid);
        if (detector.waitFor(tid, pid)) {
            throw new TransactionAbortedException();
        }
        lock.writeLock(tid);
        detector.lockGranted(tid, pid);
    }

    public  void releaseLock(PageId pid, TransactionId tid) {
        getLock(pid).releaseLock(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        return getLock(p).owners.contains(tid);
    }

    public Set<PageId> lockedWritePages(TransactionId tid) {
        return locks.entrySet().stream()
            .filter(entry -> entry.getValue().owners.contains(tid))
            .filter(entry -> !entry.getValue().lockingRead)
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
    }

    public Set<PageId> lockedReadPages(TransactionId tid) {
        return locks.entrySet().stream()
            .filter(entry -> entry.getValue().owners.contains(tid))
            .filter(entry -> entry.getValue().lockingRead)
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
    }

    private synchronized Lock getLock(PageId pid) {
        Lock lock = locks.get(pid);
        if (lock == null) {
            lock = new Lock();
            locks.put(pid, lock);
        }
        return lock;
    }

    class Lock {
        private volatile Set<TransactionId> owners = ConcurrentHashMap.newKeySet();
        private volatile boolean lockingRead = true;

        public void readLock(TransactionId tid) {
            if (owners.isEmpty()) {
                lockingRead = true;
            }
            if (lockingRead) {
                owners.add(tid);
                return;
            }
            if (canUpgradeOrReenter(tid)) {
                return;
            }
            // 有人持有writeLock
            while (!owners.isEmpty() && !canUpgradeOrReenter(tid)) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            readLock(tid);
        }

        public void writeLock(TransactionId tid) {
            if (owners.isEmpty()) {
                lockingRead = false;
                owners.add(tid);
                return;
            }
            // 有人持有readLock
            while (!owners.isEmpty() && !canUpgradeOrReenter(tid)) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            lockingRead = false;
            owners.add(tid);
        }

        public synchronized void releaseLock(TransactionId tid) {
            owners.remove(tid);
        }

        private boolean canUpgradeOrReenter(TransactionId tid) {
            // 如果持有者是同一个可以升级
            // 如果tid已经持有写锁，那么默认获取读锁
            return owners.size() == 1 && owners.contains(tid);
        }
    }

    class DeadLockDetector {
        private Map<TransactionId, Set<PageId>> waitForMap = new ConcurrentHashMap<>();


        public synchronized boolean waitFor(TransactionId tid, PageId pid) {
            waitForMap.compute(tid, (k, v)->{
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(pid);
                return v;
            });

            Lock lock = locks.get(pid);
            
            if (hasLoop(lock.owners)) {
                // 不让他等待，也就是他不在等待了，和拿到锁是一样的
                lockGranted(tid, pid);
                return true;
            }
            return false;
        }

        public synchronized void lockGranted(TransactionId tid, PageId pid) {
            Set<PageId> waiting = waitForMap.get(tid);
            waiting.remove(pid);
            if (waiting.isEmpty()) {
                waitForMap.remove(tid);
            }
        }

        private boolean hasLoop(Set<TransactionId> waiters) {
            // bfs
            Set<TransactionId> checked = new HashSet<>();
            Queue<TransactionId> queue = new LinkedList<>();
            queue.addAll(waiters);

            while(!queue.isEmpty()) {
                TransactionId current = queue.poll();
                checked.add(current);

                Set<PageId> waiting = waitForMap.get(current);
                if (waiting == null) {
                    continue;
                }
                for (PageId pId : waiting) {
                    for (TransactionId t : locks.get(pId).owners) {
                        // 只有正在等待锁的事务才需要检查
                        if (!waitForMap.containsKey(t)) {
                            continue;
                        }
                        // 自己占有的锁可以重入
                        if (current.equals(t)) {
                            continue;
                        }
                        if (checked.contains(t)) {
                            // 有环！
                            return true;
                        } else {
                            queue.add(t);
                        }
                    }
                }
            }
            return false;
        }
    }
}
