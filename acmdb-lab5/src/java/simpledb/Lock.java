package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Lock {
    private enum LockType {
        SHARED,
        EXCLUSIVE
    }

    private LockType lockType;
    private PageId lockingPageId;
    // a mutual exlusive lock
    private Set<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;

    public Lock(PageId lockingPageId) {
        this.lockType = null;
        this.lockingPageId = lockingPageId;
        this.sharedLocks = ConcurrentHashMap.newKeySet();
        this.exclusiveLock = null;
    }

    public boolean is_exclusive() {
        return exclusiveLock != null || lockType == LockType.EXCLUSIVE;
    }

    public boolean acquire_lock(Permissions perm, TransactionId tid) {
        // spin lock
        if (perm.equals(Permissions.READ_ONLY)) {
            // shared lock
            if (exclusiveLock == null || exclusiveLock.equals(tid)) {
                lockType = (exclusiveLock == null) ? LockType.SHARED : LockType.EXCLUSIVE;
                sharedLocks.add(tid);
                return true;
            }
            else
                return false;
        } else if (perm.equals(Permissions.READ_WRITE)){
            // exclusive lock
            HashSet<TransactionId> toNodes = get_to_nodes();
            if (toNodes.size() == 0 || (toNodes.size() == 1 && toNodes.contains(tid))) {
                lockType = LockType.EXCLUSIVE;
                exclusiveLock = tid;
                return true;
            }
            else
                return false;
        } else
            throw new RuntimeException("unkown permission at acquire_lock");
    }

    public void release_lock(TransactionId tid){
        assert exclusiveLock == null || tid.equals(exclusiveLock);
        if (tid.equals(exclusiveLock)) {
            exclusiveLock = null;
        }
        else {
            sharedLocks.remove(tid);
        }
    }

    public boolean holding_lock(TransactionId tid){
        return tid.equals(exclusiveLock) || sharedLocks.contains(tid);
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
