package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    ConcurrentHashMap<TransactionId, HashSet<PageId>> tid2PagesTable;
    ConcurrentHashMap<PageId, Lock> pid2LockTable;
    DependencyGraph dpGraph; // used to detect dead lock

    public LockManager(){
        this.pid2LockTable = new ConcurrentHashMap<>();
        this.tid2PagesTable = new ConcurrentHashMap<>();
        this.dpGraph = new DependencyGraph();
    }

    private static class DependencyGraph {
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges;

        public DependencyGraph(){
            this.edges = new ConcurrentHashMap<>();
        }

        public void updateEdges(TransactionId tid, PageId start){

        }

        public boolean hasCycle(){
            return false;
        }
    }

    public void acquire_lock(Permissions perm, TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (! pid2LockTable.containsKey(pid))
            pid2LockTable.put(pid, new Lock(pid));
        boolean get_lock = false;
        synchronized (pid2LockTable.get(pid)) {
            get_lock = pid2LockTable.get(pid).acquire_lock(perm, tid);
        }
        while (! get_lock) {
            // can't get lock, just wait
            Thread.yield();
            // here this thread is notified by some others, which means dpGraph may be updated
            dpGraph.updateEdges(tid, pid);
            if (dpGraph.hasCycle())
                // find dead lock
                throw new TransactionAbortedException("DeadLock Detected");
            // check dead lock
            synchronized (pid2LockTable.get(pid)) {
                get_lock = pid2LockTable.get(pid).acquire_lock(perm, tid);
            }
        }
        
        tid2PagesTable.putIfAbsent(tid, new HashSet<>());
        tid2PagesTable.get(tid).add(pid); // this Transaction holds pid
    }

    public void release_lock(TransactionId tid, PageId pid) {
        synchronized (pid2LockTable.get(pid)) {
            pid2LockTable.get(pid).release_lock(tid);
        }
        if (tid2PagesTable.containsKey(tid))
            tid2PagesTable.get(tid).remove(pid);
    }

    public boolean holding_lock(TransactionId tid, PageId pid){
        synchronized (pid2LockTable.get(pid)) {
            return pid2LockTable.get(pid).holding_lock(tid);
        }
    }

    public boolean is_exclusive(PageId pid) {
        return pid2LockTable.get(pid).is_exclusive();
    }

    public HashSet<PageId> transactionComplete(TransactionId tid, boolean commit){
        HashSet<PageId> lockedPages = tid2PagesTable.get(tid);
        tid2PagesTable.remove(tid);
        return lockedPages;
    }
}
