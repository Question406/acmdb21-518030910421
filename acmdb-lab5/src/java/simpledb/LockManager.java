package simpledb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
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

    private class DependencyGraph {
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges;

        public DependencyGraph(){
            this.edges = new ConcurrentHashMap<>();
        }

        public void updateEdges(TransactionId tid, PageId start){
            edges.putIfAbsent(tid, new HashSet<>());
            HashSet<TransactionId> newtoNodes = null;
            synchronized (pid2LockTable.get(start)) {
                newtoNodes = pid2LockTable.get(start).get_to_nodes();
            }
            edges.replace(tid, newtoNodes);
        }

        public boolean hasCycle(TransactionId start){
            // BFS
            HashSet<TransactionId> visited = new HashSet<>();
            Queue<TransactionId> queue = new LinkedList<>();
            queue.add(start);
            while (!queue.isEmpty()){
                TransactionId cur = queue.poll();
                HashSet<TransactionId> toNodes = edges.get(cur);
                if (toNodes == null)
                    continue;
                for (TransactionId toNode : toNodes) {
                    if (toNode.equals(start))
                        // found cycle
                        return true;
                    if (! visited.contains(toNode)){
                        queue.add(toNode);
                        visited.add(toNode);
                    }
                }
            }
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
            if (dpGraph.hasCycle(tid))
                // find dead lock
                throw new TransactionAbortedException("Dead lock detected");
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
