package simpledb;

import java.util.HashSet;

public class Lock {
    private enum LockType {
        SHARED,
        EXCLUSIVE
    }

    private LockType lockType;
    private PageId lockingPageId;
    // a mutual exlusive lock
    private HashSet<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;

    public Lock(PageId lockingPageId) {
        this.lockType = null;
        this.lockingPageId = lockingPageId;
        this.sharedLocks = new HashSet<>();
        this.exclusiveLock = null;
    }

    public boolean is_exclusive() {
        return lockType == LockType.EXCLUSIVE;
    }

    public boolean acquire_lock(Permissions perm, TransactionId tid) {
        if (perm.equals(Permissions.READ_ONLY)) {
            if (exclusiveLock != null) {
                // some tid may be writing
                return exclusiveLock.equals(tid);
            }
            this.lockType = LockType.SHARED;
            sharedLocks.add(tid);
            return true;
        } else if (perm.equals(Permissions.READ_WRITE)){
            if (exclusiveLock != null) {
                return exclusiveLock.equals(tid);
            }
            if (sharedLocks.size() > 1)
                // can't be the same tid
                return false;
            if (sharedLocks.isEmpty() || sharedLocks.contains(tid)) {
                // the same tid, change from shared to exclusive
                exclusiveLock = tid;
                sharedLocks.clear();
                this.lockType = LockType.EXCLUSIVE;
                return true;
            }
            return false;
        } else
            throw new RuntimeException("unkown permission at acquire_lock");
    }

    public void release_lock(TransactionId tid){
        assert exclusiveLock == null || tid.equals(exclusiveLock);
        if (tid.equals(exclusiveLock)) {
            assert lockType == LockType.EXCLUSIVE;
            exclusiveLock = null;
        }
        else {
            assert lockType == LockType.SHARED;
            sharedLocks.remove(tid);
        }
    }

    public boolean holding_lock(TransactionId tid){
        if (lockType == LockType.EXCLUSIVE)
            return exclusiveLock.equals(tid);
        else if (lockType == LockType.SHARED)
            return sharedLocks.contains(tid);
        else
            throw new RuntimeException("Unknown lock type @holding_lock");
    }
    
    public HashSet<TransactionId> get_to_nodes(){
        HashSet<TransactionId> transactionIds = new HashSet<>(sharedLocks);
        if (exclusiveLock != null) {
            assert sharedLocks.isEmpty();
            transactionIds.add(exclusiveLock);
        }
        return transactionIds;
    }
}
