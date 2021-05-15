package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    ConcurrentHashMap<TransactionId, HashSet<PageId>> tidPageHoldTable;
    ConcurrentHashMap<PageId, Lock> tid2LockTable;
    DependencyGraph dpGraph; // used to detect dead lock

    public LockManager(){
        this.tid2LockTable = new ConcurrentHashMap<>();
        this.tidPageHoldTable = new ConcurrentHashMap<>();
        this.dpGraph = new DependencyGraph();
    }

    private class DependencyGraph {
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges;

        public DependencyGraph(){
            this.edges = new ConcurrentHashMap<>();
        }
    }

    public boolean acquire_lock() {
        return false;
    }

}
