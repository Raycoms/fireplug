package main.java.com.bag.operations;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import main.java.com.bag.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.Serializable;

/**
 * Update command which may be sent to the database.
 */
public class UpdateOperation<S extends Serializable> implements IOperation, Serializable
{
    private final S key;
    private final S value;

    /**
     * Default constructor for kryo.
     */
    public UpdateOperation(){ key = value = null;}

    public UpdateOperation(final S key, final S value)
    {
        this.key = key;
        this.value = value;
    }

    @Override
    public void apply(final IDatabaseAccess access, final long snapshotId, final RSAKeyLoader keyLoader, final int idClient)
    {
        try
        {
            if (key instanceof NodeStorage)
            {
                final NodeStorage tempStorage = (NodeStorage) value;
                access.applyUpdate((NodeStorage) key, tempStorage, snapshotId, idClient);
            }
            else if (value instanceof RelationshipStorage)
            {
                final RelationshipStorage tempStorage = (RelationshipStorage) value;
                access.applyUpdate((RelationshipStorage) key, tempStorage, snapshotId, idClient);
            }
            else
            {
                Log.getLogger().error("Trying to update incorrect type in the database.");
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Unable to sign nodeStorage ", e);
        }
    }

    /**
     * Get the Storage object.
     * @return it.
     */
    public Object getKey()
    {
        return key;
    }

    /**
     * Get the Storage object.
     * @return it.
     */
    public Object getValue()
    {
        return value;
    }

    @Override
    public int hashCode()
    {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(final Object e)
    {
        if((key instanceof NodeStorage && e instanceof NodeStorage) || (key instanceof RelationshipStorage && e instanceof RelationshipStorage))
        {
            return key.equals(e);
        }
        else if(key instanceof NodeStorage && e instanceof RelationshipStorage)
        {
            return key.equals(((RelationshipStorage) e).getStartNode()) || key.equals(((RelationshipStorage) e).getEndNode());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Update: " + key.toString() + " to " + value.toString();
    }
}
