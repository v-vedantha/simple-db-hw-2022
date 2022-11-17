package simpledb.storage;
import java.util.*;

import simpledb.common.Database;
import simpledb.transaction.*;
import java.util.concurrent.*;

// Apparently the syncrhonized keyword makes it so that only one of these can run at atime.

public class LockManager {
    
    public Map<PageId, Queue<Lock>> lockTable;
    private int randomForDebugging;

    public LockManager() {
        // System.out.println("made a lock manager");
        lockTable = new ConcurrentHashMap<>();
        // set randome to something random
        Random rand = new Random();
        randomForDebugging = rand.nextInt(100);
    }

    public synchronized boolean detectDeadlock(TransactionId tid, PageId pid, Lock.LockType type, Set<TransactionId> visited) {
        if (visited.contains(tid)) {
            return true;
        }
        visited.add(tid);
        if (lockTable.containsKey(pid)) {
            for (Lock lock : lockTable.get(pid)) {
                if (type == Lock.LockType.SHARED && lock.getType() == Lock.LockType.SHARED) {
                    continue;
                }
                if (lock.getType() == Lock.LockType.EXCLUSIVE) {
                    if (detectDeadlock(lock.getTid(), pid, Lock.LockType.EXCLUSIVE, visited)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public synchronized boolean grantSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException 
    {
        if (lockTable.containsKey(pid))
        {
            Queue<Lock> locks = lockTable.get(pid);
            locks.add(new Lock(Lock.LockType.SHARED, tid, pid));
            for (Lock lock : locks)
            {
                if (lock.getType() == Lock.LockType.EXCLUSIVE && lock.getTid() != tid)
                {
                    if (detectDeadlock(tid, pid, Lock.LockType.SHARED, new HashSet<TransactionId>()))
                    {
                        // System.out.println("Decided not to give a shared lock");
                        Database.getBufferPool().transactionComplete(tid, false);
                        throw new TransactionAbortedException();
                    }
                    else{
                        return false;
                    }
                }
            }
        }
        else
        {
            Queue<Lock> locks = new ConcurrentLinkedQueue<Lock>();
            locks.add(new Lock(Lock.LockType.SHARED, tid, pid));
            lockTable.put(pid, locks);
        }
        // System.out.println("Granted shared lock to " + tid.getId() + " on " + pid.getPageNumber());
        return true;
    }

    public synchronized boolean grantExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
        if (lockTable.containsKey(pid))
        {
            Queue<Lock> locks = lockTable.get(pid);
            locks.add(new Lock(Lock.LockType.EXCLUSIVE, tid, pid));
            for (Lock lock : locks)
            {
                if (lock.getTid() != tid)
                {
                    if (detectDeadlock(tid, pid, Lock.LockType.EXCLUSIVE, new HashSet<TransactionId>()))
                    {
                        // System.out.println("Decided not to give a exclusive lock");
                        // abortion time
                        Database.getBufferPool().transactionComplete(tid, false);
                        throw new TransactionAbortedException();
                    }
                    else{
                        return false;
                    }
                }
            }
        }
        else
        {
            Queue<Lock> locks = new ConcurrentLinkedQueue<Lock>();
            locks.add(new Lock(Lock.LockType.EXCLUSIVE, tid, pid));
            lockTable.put(pid, locks);
        }
        // System.out.println("Granted exc lock to " + tid.getId() + " on " + pid.getPageNumber());
        return true;
    }

    public synchronized boolean releaseLocks(TransactionId tid, PageId pid)
    {
        if (lockTable.containsKey(pid))
        {
            Queue<Lock> locks = lockTable.get(pid);
            for (Lock lock : locks)
            {
                if (lock.getTid() == tid)
                {
                    locks.remove(lock);
                }
            }
            if (locks.size() == 0)
            {
                lockTable.remove(pid);
            }
            return true;
        }
        return false;
    }

    public synchronized boolean releaseAllLocks(TransactionId tid)
    {
        for (PageId pid : lockTable.keySet())
        {
            releaseLocks(tid, pid);
        }
        return true;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid)
    {
        if (lockTable.containsKey(pid))
        {
            Queue<Lock> locks = lockTable.get(pid);
            for (Lock lock : locks)
            {
                if (lock.getTid() == tid)
                {
                    return true;
                }
            }
        }
        return false;
    }


}
