package cp1.solution;

import cp1.base.*;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Semaphore;

public class TransactionManagerImpl implements TransactionManager {

    private final Map<ResourceId, Resource> resources;
    private final LocalTimeProvider timeProvider;
    private final Map<Thread, TransactionInfo> transactions;
    private final Map<ResourceId, Thread> whatThreadHasLockOnResource;
    private final Map<ResourceId, Set<Thread>>
            whatThreadsWaitForResources;
    private final Object lockObject = new Object();

    public TransactionManagerImpl(Collection<Resource> resources,
                                  LocalTimeProvider timeProvider) {
        this.resources = new HashMap<>();
        for (Resource resource : resources) {
            this.resources.put(resource.getId(), resource);
        }

        this.timeProvider = timeProvider;
        this.transactions = new HashMap<>();
        this.whatThreadHasLockOnResource = new HashMap<>();
        this.whatThreadsWaitForResources = new HashMap<>();
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {

        synchronized (lockObject) {
            if (isTransactionActive()) {
                throw new AnotherTransactionActiveException();
            }

            long time = timeProvider.getTime();
            TransactionInfo transactionInfo = new TransactionInfo(time);

            transactions.put(Thread.currentThread(), transactionInfo);
        }
    }

    private boolean checkIfFirstThreadYounger(Thread thread1, Thread thread2) {

        synchronized (lockObject) {
            long thread1StartTime = transactions.get(thread1).getStartTime();
            long thread2StartTime = transactions.get(thread2).getStartTime();

            if (thread1StartTime > thread2StartTime ||
                    (thread1StartTime == thread2StartTime &&
                            thread1.getId() > thread2.getId())) {
                return true;
            }
            return false;
        }
    }

    // Add currentThread to waiting threads on Resource with rid.
    // Update this thread's transactionInfo.
    private void addToWaiting(ResourceId rid) {

        synchronized (lockObject) {
            transactions.get(Thread.currentThread()).setWhatResourceItWaitsOn(rid);

            Set<Thread> resourceWaitingThreads =
                    whatThreadsWaitForResources.get(rid);
            if (resourceWaitingThreads == null) {
                resourceWaitingThreads = new HashSet<>();
                resourceWaitingThreads.add(Thread.currentThread());
                whatThreadsWaitForResources.put(rid, resourceWaitingThreads);
            } else {
                resourceWaitingThreads.add(Thread.currentThread());
            }
        }
    }

    // Remove currentThread from waiting threads on Resource with rid.
    // Update this thread's transactionInfo.
    private void removeFromWaiting(ResourceId rid) {

        synchronized (lockObject) {
            transactions.get(Thread.currentThread()).setWhatResourceItWaitsOn(null);

            Set<Thread> resourceWaitingThreads =
                    whatThreadsWaitForResources.get(rid);

            resourceWaitingThreads.remove(Thread.currentThread());

            if (resourceWaitingThreads.isEmpty()) {
                whatThreadsWaitForResources.remove(rid);
            }
        }
    }

    // Check if currentThread waiting on Resource with rid would cause deadlock.
    // If so abort youngest thread from those which would cause deadlock.
    private void removeDeadlock(ResourceId rid) {

        synchronized (lockObject) {
            Thread YoungestThread = Thread.currentThread();
            Thread ThreadWithResourceLock = whatThreadHasLockOnResource.get(rid);

            if (ThreadWithResourceLock != null) {
                ResourceId whatResourceItWaitsOn =
                        transactions.get(ThreadWithResourceLock).getWhatResourceItWaitsOn();

                while (ThreadWithResourceLock != Thread.currentThread() &&
                        whatResourceItWaitsOn != null) {

                    if (checkIfFirstThreadYounger(ThreadWithResourceLock,
                            YoungestThread)) {
                        YoungestThread = ThreadWithResourceLock;
                    }

                    ThreadWithResourceLock = whatThreadHasLockOnResource.get(whatResourceItWaitsOn);

                    whatResourceItWaitsOn =
                            transactions.get(ThreadWithResourceLock).getWhatResourceItWaitsOn();
                }

                if (ThreadWithResourceLock == Thread.currentThread()) { // deadlock
                    YoungestThread.interrupt();
                    TransactionInfo YoungestThreadInfo =
                            transactions.get(YoungestThread);
                    YoungestThreadInfo.setTransactionAborted(true);
                    YoungestThreadInfo.getDelay().release();
                }
            }
        }
    }

    @Override
    public void operateOnResourceInCurrentTransaction(
            ResourceId rid,
            ResourceOperation operation
    ) throws
            NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException
    {

        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }

        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }
        
        synchronized (lockObject) {
            if (!resources.containsKey(rid)) {
                throw new UnknownResourceIdException(rid);
            }
        }

        boolean resourceOccupied;
        Semaphore delay;

        // Try to get lock on resource.
        synchronized (lockObject) {
            TransactionInfo transactionInfo =
                    transactions.get(Thread.currentThread());
            delay = transactionInfo.getDelay();

            if (whatThreadHasLockOnResource.get(rid) == Thread.currentThread()) {
                resourceOccupied = false;
            } else if (whatThreadHasLockOnResource.get(rid) == null) {
                resourceOccupied = false;
                whatThreadHasLockOnResource.put(rid, Thread.currentThread());
                transactionInfo.getWhatResourcesThreadHasLockOn().add(rid);
            } else {
                resourceOccupied = true;
                addToWaiting(rid);
            }
        }

        if (resourceOccupied) {
            removeDeadlock(rid);

            try {
                delay.acquire();
            } catch (InterruptedException e) {
                synchronized (lockObject) {
                    if (transactions.get(Thread.currentThread()).isTransactionAborted()) {
                        throw new ActiveTransactionAborted();
                    } else {
                        throw new InterruptedException();
                    }
                }
            } finally {
                removeFromWaiting(rid);
            }
        }

        operation.execute(resources.get(rid));
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        // Add operation to transactionHistory.
        synchronized (lockObject) {
            Deque<TransactionHistoryElement> transactionHistory =
                    transactions.get(Thread.currentThread()).getTransactionHistory();

            transactionHistory.add(new TransactionHistoryElement(rid,
                    operation));
        }
    }

    // Remove Transaction data.
    // If there are threads waiting on Resources wake them up (1 per resource)
    // and give them locks on those resources.
    // Otherwise release locks on resources.
    private void finishCurrentTransaction() {

        synchronized (lockObject) {
            TransactionInfo transactionInfo = transactions.get(Thread.currentThread());
            BlockingDeque<ResourceId> whatResourcesThreadHasLockOn =
                    transactionInfo.getWhatResourcesThreadHasLockOn();

            for (ResourceId rid : whatResourcesThreadHasLockOn) {
                whatThreadHasLockOnResource.remove(rid);

                Set<Thread> whatThreadsWaitForResource =
                        whatThreadsWaitForResources.get(rid);

                if (whatThreadsWaitForResource != null) {
                    if (whatThreadsWaitForResource.isEmpty()) {
                        whatThreadsWaitForResources.remove(rid);
                    } else {
                        Thread threadToBeWoken =
                                whatThreadsWaitForResource.iterator().next();

                        whatThreadHasLockOnResource.put(rid, threadToBeWoken);

                        TransactionInfo toBeWokenTransactionInfo =
                                transactions.get(threadToBeWoken);

                        toBeWokenTransactionInfo.getWhatResourcesThreadHasLockOn().add(rid);

                        toBeWokenTransactionInfo.getDelay().release();
                    }
                }
            }
            transactions.remove(Thread.currentThread());
        }
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException,
            ActiveTransactionAborted {

        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }

        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        finishCurrentTransaction();
    }

    @Override
    public void rollbackCurrentTransaction() {

        if (!isTransactionActive()) {
            return;
        }

        BlockingDeque<TransactionHistoryElement> transactionHistory;

        synchronized (lockObject) {
            transactionHistory =
                    transactions.get(Thread.currentThread()).getTransactionHistory();
        }

        Iterator<TransactionHistoryElement> descendingIterator =
                transactionHistory.descendingIterator();

        ResourceId rid;
        Resource resource;
        ResourceOperation operation;

        // Rollback all operations.
        while (descendingIterator.hasNext()) {
            TransactionHistoryElement  transactionHistoryElement =
                    descendingIterator.next();

            rid = transactionHistoryElement.getRid();
            operation = transactionHistoryElement.getOperation();

            synchronized (lockObject) {
                resource = resources.get(rid);
            }

            operation.undo(resource);
        }

        finishCurrentTransaction();
    }

    @Override
    public boolean isTransactionActive() {
        synchronized (lockObject) {
            return transactions.containsKey(Thread.currentThread());
        }
    }

    @Override
    public boolean isTransactionAborted() {
        synchronized (lockObject) {
            return transactions.get(Thread.currentThread()).isTransactionAborted();
        }
    }
}
