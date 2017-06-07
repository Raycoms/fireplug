package main.java.com.bag.util.storage;

import main.java.com.bag.operations.IOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * Class containing all necessary info a slave needs to execute updates.
 */
public class SlaveUpdateStorage
{
    private final int global;
    private final List<IOperation> operations;

    /**
     * Create an instance of the slave update storage.
     * @param global the global id which sent the update.
     * @param operations the operations associated with the transaction.
     */
    public SlaveUpdateStorage(final int global, final List<IOperation> operations)
    {
        this.global = global;
        this.operations = operations;
    }

    /**
     * Getter for the global id.
     * @return the global id.
     */
    public int getGlobalId()
    {
        return global;
    }

    /**
     * Getter for the operations.
     * @return a copy of the operations.
     */
    public List<IOperation> getOperations()
    {
        return new ArrayList<>(operations);
    }
}
