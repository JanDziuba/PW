package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

public class TransactionHistoryElement {

    private final ResourceId rid;
    private final ResourceOperation operation;

    public TransactionHistoryElement(ResourceId rid,
                                     ResourceOperation operation) {
        this.rid = rid;
        this.operation = operation;
    }

    public ResourceId getRid() {
        return rid;
    }

    public ResourceOperation getOperation() {
        return operation;
    }
}
