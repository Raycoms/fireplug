package main.java.com.bag.operations;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Delete command which may be sent to the database.
 */
public class DeleteOperation<S extends Serializable> implements IOperation, Serializable
{
    private final S storage;

    /**
     * Default constructor for kryo.
     */
    public DeleteOperation(){ storage = null;}

    public DeleteOperation(final S key)
    {
        this.storage = key;
    }

    @Override
    public boolean apply(@NotNull final IDatabaseAccess access, long snapshotId)
    {
        if(storage instanceof NodeStorage)
        {
            return access.applyDelete((NodeStorage) storage, snapshotId);
        }
        else if(storage instanceof RelationshipStorage)
        {
            return access.applyDelete((RelationshipStorage) storage, snapshotId);
        }
        else
        {
            Log.getLogger().warn("Trying to delete incorrect type in the database.");
        }
        return false;
    }

    /**
     * Get the Storage object.
     * @return it.
     */
    public Object getObject()
    {
        return storage;
    }

    @Override
    public int hashCode()
    {
        return storage == null ? 0 : storage.hashCode();
    }

    @Override
    public boolean equals(Object e)
    {
        if((storage instanceof NodeStorage && e instanceof NodeStorage) || (storage instanceof RelationshipStorage && e instanceof RelationshipStorage))
        {
            return storage.equals(e);
        }
        else if(storage instanceof NodeStorage && e instanceof RelationshipStorage)
        {
            return storage.equals(((RelationshipStorage) e).getStartNode()) || storage.equals(((RelationshipStorage) e).getEndNode());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Delete: " + storage.toString();
    }
}