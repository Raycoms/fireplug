package main.java.com.bag.operations;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import bftsmart.tom.util.TOMUtil;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.Serializable;
import java.util.Arrays;

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
    public void apply(final IDatabaseAccess access, long snapshotId, final RSAKeyLoader keyLoader, final int idClient)
    {
        final byte[] signature;
        try
        {
            if (storage instanceof NodeStorage)
            {
                final NodeStorage tempStorage = (NodeStorage) storage;
                signature = TOMUtil.signMessage(keyLoader.loadPrivateKey(), tempStorage.getBytes());
                tempStorage.addProperty("signature" + idClient, new String(signature, "UTF-8"));
                access.applyCreate( tempStorage, snapshotId);
            }
            else if (storage instanceof RelationshipStorage)
            {
                final RelationshipStorage tempStorage = (RelationshipStorage) storage;
                /*signature = TOMUtil.signMessage(keyLoader.loadPrivateKey(), tempStorage.getBytes());
                tempStorage.addProperty("signature" + idClient, new String(signature, "UTF-8"));*/
                access.applyCreate( tempStorage, snapshotId);
            }
            else
            {
                Log.getLogger().warn("Trying to create incorrect type in the database.");
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().warn("Unable to sign nodeStorage ", e);
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
    public boolean equals(Object e)
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
