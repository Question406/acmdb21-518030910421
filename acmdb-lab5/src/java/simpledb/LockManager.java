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

        tid2PagesTable.putIfAbsent(tid, new HashSet<>());
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

    public void release_pages(TransactionId tid, HashSet<PageId> todoPages) {
        // first release page, then delete lock
//        System.out.println(String.format("%s try release %s pages", tid, todoPages.size()));
        for (PageId todo : todoPages){
            release_page(tid, todo);
        }
        tid2PagesTable.remove(tid);
        dpGraph.clearEdges(tid);
        dpGraph.edges.remove(tid);
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
//        tid2PagesTable.remove(tid);
//        dpGraph.clearEdges(tid);
//        dpGraph.edges.remove(tid);
        return lockedPages;
    }
}

//package simpledb;
//
//import java.util.*;
//import java.util.concurrent.*;
//
//public class LockManager {
//    private final ConcurrentHashMap<PageId, Object> locks;
//    private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
//    private final ConcurrentHashMap<PageId, ConcurrentLinkedDeque<TransactionId>> sharedLocks;
//    private final ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> transactionHoldLocks;
//    private final ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> transactionHoldXLocks;
//    private final ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<TransactionId>> dependencyGraph;
//
//    public LockManager() {
//        locks = new ConcurrentHashMap<>();
//        sharedLocks = new ConcurrentHashMap<>();
//        exclusiveLocks = new ConcurrentHashMap<>();
//        transactionHoldLocks = new ConcurrentHashMap<>();
//        transactionHoldXLocks = new ConcurrentHashMap<>();
//        dependencyGraph = new ConcurrentHashMap<>();
//    }
//
//    public static LockManager GetLockManager() {
//        return new LockManager();
//    }
//
//
//    private boolean hasLock(TransactionId tid, PageId pid, boolean isReadOnly) {
//        if (exclusiveLocks.containsKey(pid) && tid.equals(exclusiveLocks.get(pid))) {
//            return true;
//        }
//        return isReadOnly && sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid);
//    }
//
//    private Object getLock(PageId pid) {
//        locks.putIfAbsent(pid, new Object());
//        return locks.get(pid);
//    }
//
//    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
//        if (perm == Permissions.READ_ONLY) {
//            if (hasLock(tid, pid, true)) return true;
//            acquireSLock(tid, pid);
//        } else if (perm == Permissions.READ_WRITE) {
//            if (hasLock(tid, pid, false)) return true;
//            acquireXLock(tid, pid);
//        }
//        updateTransactionLocks(tid, pid);
//        return true;
//    }
//
//    private void acquireSLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
//        Object lock = getLock(pid);
//
//        while (true) {
//            synchronized (lock) {
//                TransactionId holder = exclusiveLocks.get(pid);
//                boolean notBlocked = (holder == null || holder.equals(tid));
//
//                if (notBlocked) {
//                    removeDependency(tid);
//                    addSTransaction(pid, tid);
//                    return;
//                }
//                ArrayList<TransactionId> holders = new ArrayList<>();
//                holders.add(holder);
//                updateDependency(tid, holders);
//            }
//        }
//    }
//
//    private void acquireXLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
//        Object lock = getLock(pid);
//
//        while (true) {
//            synchronized (lock) {
//                ArrayList<TransactionId> holders = new ArrayList<>();
//                if (exclusiveLocks.containsKey(pid)) {
//                    holders.add(exclusiveLocks.get(pid));
//                }
//                if (sharedLocks.containsKey(pid)) {
//                    holders.addAll(sharedLocks.get(pid));
//                }
//
//                boolean notBlocked = holders.size() == 0 || (holders.size() == 1 && holders.get(0).equals(tid));
//
//                if (notBlocked) {
//                    removeDependency(tid);
//                    addXTransaction(pid, tid);
//                    return;
//                }
//                updateDependency(tid, holders);
//            }
//        }
//    }
//
//    private void addSTransaction(PageId pid, TransactionId tid) {
//        sharedLocks.putIfAbsent(pid, new ConcurrentLinkedDeque<>());
//        sharedLocks.get(pid).add(tid);
//    }
//
//    private void addXTransaction(PageId pid, TransactionId tid) {
//        exclusiveLocks.put(pid, tid);
//        transactionHoldXLocks.putIfAbsent(tid, new ConcurrentLinkedDeque<>());
//        transactionHoldXLocks.get(tid).add(pid);
//    }
//
//
//    private void removeDependency(TransactionId tid) {
//        synchronized (dependencyGraph) {
//            dependencyGraph.remove(tid);
//            for (TransactionId curtid : dependencyGraph.keySet()) {
//                dependencyGraph.get(curtid).remove(tid);
//            }
//        }
//    }
//
//    public boolean is_exclusive(PageId pid){
//        return exclusiveLocks.containsKey(pid);
//    }
//
//    private void updateDependency(TransactionId acquirer, ArrayList<TransactionId> holders)
//            throws TransactionAbortedException {
//        dependencyGraph.putIfAbsent(acquirer, new ConcurrentLinkedDeque<>());
//        boolean hasChange = false;
//        ConcurrentLinkedDeque<TransactionId> childs = dependencyGraph.get(acquirer);
//        for (TransactionId holder : holders) {
//            if (!childs.contains(holder) && !holder.equals(acquirer)) {
//                hasChange = true;
//                dependencyGraph.get(acquirer).add(holder);
//            }
//        }
//        if (hasChange) {
//            checkDeadLock(acquirer, new HashSet<>());
//        }
//    }
//
//
//    private void checkDeadLock(TransactionId root, HashSet<TransactionId> visit) throws TransactionAbortedException {
//        // DFS Checking self-loop
//        if (!dependencyGraph.containsKey(root))
//            return;
//        for (TransactionId child : dependencyGraph.get(root)) {
//            if (visit.contains(child)) {
//                throw new TransactionAbortedException();
//            }
//            visit.add(child);
//            checkDeadLock(child, visit);
//            visit.remove(child);
//        }
//    }
//
//    private void updateTransactionLocks(TransactionId tid, PageId pid) {
//        transactionHoldLocks.putIfAbsent(tid, new ConcurrentLinkedDeque<>());
//        transactionHoldLocks.get(tid).add(pid);
//    }
//
//
//    public void releasePage(TransactionId tid, PageId pid) {
//        if (holdsLock(tid, pid)) {
//            Object lock = getLock(pid);
//            synchronized (lock) {
//                if (sharedLocks.containsKey(pid)) {
//                    sharedLocks.get(pid).remove(tid);
//                }
//                if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
//                    exclusiveLocks.remove(pid);
//                }
//                if (transactionHoldLocks.containsKey(tid)) {
//                    transactionHoldLocks.get(tid).remove(pid);
//                }
//                if (transactionHoldXLocks.containsKey(tid)) {
//                    transactionHoldXLocks.get(tid).remove(pid);
//                }
//            }
//        }
//    }
//
//    public void releasePages(TransactionId tid) {
//        if (transactionHoldLocks.containsKey(tid)) {
//            for (PageId pid : transactionHoldLocks.get(tid)) {
//                releasePage(tid, pid);
//            }
//        }
//        transactionHoldXLocks.remove(tid);
//    }
//
//    public boolean holdsLock(TransactionId tid, PageId pid) {
//        return hasLock(tid, pid, true) || hasLock(tid, pid, false);
//    }
//
//    public ConcurrentLinkedDeque<PageId> getTransactionDirtiedPages(TransactionId tid) {
//        return transactionHoldXLocks.get(tid);
//    }
//}