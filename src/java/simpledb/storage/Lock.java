package simpledb.storage;
import java.util.*;
import simpledb.transaction.*;

public class Lock {

    public enum LockType {
        SHARED, EXCLUSIVE
    }

    private LockType type;
    private TransactionId tid;
    private PageId pid;

    public Lock(LockType type, TransactionId tid, PageId pid) {
        this.type = type;
        this.tid = tid;
        this.pid = pid;
    }

    public LockType getType() {
        return type;
    }

    public TransactionId getTid() {
        return tid;
    }

    public PageId getPid() {
        return pid;
    }


    @Override
    public String toString() {
        return "Lock thing";
    }
    
}
