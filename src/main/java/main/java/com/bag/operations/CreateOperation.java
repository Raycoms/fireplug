package main.java.com.bag.operations;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.Serializable;

/**
 * Create command which may be sent to the database.
 */
public class CreateOperation<S extends Serializable> implements Operation, Serializable
{
    private final S storage;

    /**
     * Default constructor for kryo.
     */
    public CreateOperation(){ storage = null;}

    public CreateOperation(final S key)
    {
        this.storage = key;
    }

    @Override
    public void apply(final IDatabaseAccess access, long snapshotId)
    {
        if(storage instanceof NodeStorage)
        {
            access.applyCreate((NodeStorage) storage, snapshotId);
        }
        else if(storage instanceof RelationshipStorage)
        {
            access.applyCreate((RelationshipStorage) storage, snapshotId);
        }
        else
        {
            Log.getLogger().warn("Trying to create incorrect type in the database.");
        }
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
        return false;
    }
}
