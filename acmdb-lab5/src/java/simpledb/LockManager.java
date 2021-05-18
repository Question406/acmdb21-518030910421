package simpledb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LockManager {
    ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> tid2PagesTable;
    ConcurrentHashMap<PageId, Lock> pid2LockTable;
    DependencyGraph dpGraph; // used to detect dead lock

    public LockManager(){
        this.pid2LockTable = new ConcurrentHashMap<>();
        this.tid2PagesTable = new ConcurrentHashMap<>();
        this.dpGraph = new DependencyGraph();
    }

    private class DependencyGraph {
        private final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edges;

        public DependencyGraph(){
            this.edges = new ConcurrentHashMap<>();
        }

        public void debug(){
            StringBuilder stringBuilder = new StringBuilder();
            for (TransactionId tid : edges.keySet()){
                stringBuilder.append(tid);
                stringBuilder.append(" : ");
                for (TransactionId x : edges.get(tid)) {
                    stringBuilder.append(x);
                    stringBuilder.append(",");
                }
                stringBuilder.append("\n");
            }
//            System.out.println(stringBuilder);
        }

        public boolean updateEdges(TransactionId tid, Lock lock){
            synchronized (edges) {
                edges.putIfAbsent(tid, new HashSet<>());
                HashSet<TransactionId> newtoNodes = null;
                HashSet<TransactionId> nowNodes = edges.get(tid);
                newtoNodes = lock.get_to_nodes();
                boolean res = false; // check whether changed to decrease deadlock check
                for (var newnode : newtoNodes) {
                    if (!nowNodes.contains(newnode) && !tid.equals(newnode)) {
                        edges.get(tid).add(newnode);
                        res = true;
                    }
                }
                if (res) {
//                    System.out.println(String.format("after %s update", tid));
//                    debug();
                }
                return res;
            }
        }

        public void clearEdges(TransactionId tid) {
            synchronized (edges) {
                edges.putIfAbsent(tid, new HashSet<>());
                edges.get(tid).clear();
                for (var key : edges.keySet())
                    if (edges.get(key).contains(tid)) {
//                        System.out.println("hello");
                        edges.get(key).remove(tid);
                    }
//                System.out.println(String.format("after %s clear", tid));
//                debug();
            }
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
//        if (holding_lock(tid, pid)) // already holding the lock
//            return;
        //try to get
//        System.out.println(String.format("%s try %s as %s", pid, tid, perm));
        Lock lock = pid2LockTable.get(pid);
        while (true) {
            // spin lock
            synchronized (lock) {
                    boolean get_lock = lock.acquire_lock(perm, tid);
                    if (get_lock) {
//                        System.out.println(String.format("lock %s to %s as %s", pid, tid, perm));
                        dpGraph.clearEdges(tid);
                        break;
                    }
                    boolean changed = dpGraph.updateEdges(tid, lock);
                    if (changed) {
                        if (dpGraph.hasCycle(tid))
                            throw new TransactionAbortedException(String.format("%s Dead lock detected", tid));
                    }
            }
        }

        tid2PagesTable.putIfAbsent(tid, new ConcurrentLinkedDeque<>());
        tid2PagesTable.get(tid).add(pid); // this Transaction holds pid
    }

    public void release_page(TransactionId tid, PageId pid) {
//        System.out.println(String.format("%s try release %s", tid, pid));
        synchronized (pid2LockTable.get(pid)) {
            Lock lock = pid2LockTable.get(pid);
//            pid2LockTable.get(pid).release_page(tid); // release lock on page
//            System.out.println(String.format("%s get lock %s", tid, pid));
            lock.release_page(tid);
            if (tid2PagesTable.containsKey(tid))
                tid2PagesTable.get(tid).remove(pid);
//            System.out.println(String.format("%s release %s", tid, pid));
        }
    }

    public void release_pages(TransactionId tid) {
        // first release page, then delete lock
//        System.out.println(String.format("%s try release %s pages", tid, todoPages.size()));
        if (tid2PagesTable.containsKey(tid)) {
                ConcurrentLinkedDeque<PageId> todoPages = tid2PagesTable.get(tid);
                for (PageId todo : todoPages) {
                    release_page(tid, todo);
                }
                tid2PagesTable.remove(tid);
                dpGraph.clearEdges(tid);
                dpGraph.edges.remove(tid);
            }
    }

    public boolean holding_lock(TransactionId tid, PageId pid){
        synchronized (pid2LockTable.get(pid)) {
            return pid2LockTable.get(pid).holding_lock(tid);
        }
    }

    public boolean is_exclusive(PageId pid) {
        return pid2LockTable.get(pid).is_exclusive();
    }

    public ConcurrentLinkedDeque<PageId> transactionComplete(TransactionId tid, boolean commit){
        ConcurrentLinkedDeque<PageId> lockedPages = tid2PagesTable.get(tid);
        return lockedPages;
    }
}