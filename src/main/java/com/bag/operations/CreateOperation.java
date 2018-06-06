package com.bag.operations;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import com.bag.database.interfaces.IDatabaseAccess;
import com.bag.util.Log;
import com.bag.util.storage.NodeStorage;
import com.bag.util.storage.RelationshipStorage;

import java.io.Serializable;

/**
 * Create command which may be sent to the database.
 */
public class CreateOperation<S extends Serializable> implements IOperation, Serializable
{
    private final S storage;

    /**
     * Default constructor for kryo.
     */
    public CreateOperation() { storage = null;}

    public CreateOperation(final S key)
    {
        this.storage = key;
    }

    @Override
    public void apply(final IDatabaseAccess access, final long snapshotId, final RSAKeyLoader keyLoader, final int idClient)
    {
        try
        {
            if (storage instanceof NodeStorage)
            {
                final NodeStorage tempStorage = (NodeStorage) storage;
                access.applyCreate(tempStorage, snapshotId, idClient);
            }
            else if (storage instanceof RelationshipStorage)
            {
                final RelationshipStorage tempStorage = (RelationshipStorage) storage;
                access.applyCreate(tempStorage, snapshotId, idClient);
            }
            else
            {
                Log.getLogger().error("Trying to create incorrect type in the database.");
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Unable to sign nodeStorage ", e);
        }
    }

    /**
     * Get the Storage object.
     *
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
    public boolean equals(final Object e)
    {
        if ((storage instanceof NodeStorage && e instanceof NodeStorage) || (storage instanceof RelationshipStorage && e instanceof RelationshipStorage))
        {
            return storage.equals(e);
        }
        else if (storage instanceof NodeStorage && e instanceof RelationshipStorage)
        {
            return storage.equals(((RelationshipStorage) e).getStartNode()) || storage.equals(((RelationshipStorage) e).getEndNode());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Create: " + storage.toString();
    }
}
