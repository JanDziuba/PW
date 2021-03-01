package cp1.solution;

import cp1.base.ResourceId;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

public class TransactionInfo {

    // elements in chronological order
    private final BlockingDeque<TransactionHistoryElement> transactionHistory;
    private final BlockingDeque<ResourceId> whatResourcesThreadHasLockOn;
    private final long startTime;
    private final Semaphore delay;
    private boolean isTransactionAborted;
    private ResourceId whatResourceItWaitsOn;

    public TransactionInfo(long startTime) {
        this.transactionHistory = new LinkedBlockingDeque<>();
        this.whatResourcesThreadHasLockOn = new LinkedBlockingDeque<>();
        this.startTime = startTime;
        this.delay = new Semaphore(0);
        this.isTransactionAborted = false;
        this.whatResourceItWaitsOn = null;
    }

    public BlockingDeque<TransactionHistoryElement> getTransactionHistory() {
        return transactionHistory;
    }

    public BlockingDeque<ResourceId> getWhatResourcesThreadHasLockOn() {
        return whatResourcesThreadHasLockOn;
    }

    public long getStartTime() {
        return startTime;
    }

    public Semaphore getDelay() {
        return delay;
    }

    public boolean isTransactionAborted() {
        return isTransactionAborted;
    }

    public void setTransactionAborted(boolean transactionAborted) {
        isTransactionAborted = transactionAborted;
    }

    public ResourceId getWhatResourceItWaitsOn() {
        return whatResourceItWaitsOn;
    }

    public void setWhatResourceItWaitsOn(ResourceId whatResourceItWaitsOn) {
        this.whatResourceItWaitsOn = whatResourceItWaitsOn;
    }
}
