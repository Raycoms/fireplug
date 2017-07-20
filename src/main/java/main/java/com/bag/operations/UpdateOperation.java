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
    public void apply(final IDatabaseAccess access, long snapshotId, final RSAKeyLoader keyLoader, final int idClient)
    {
        final byte[] signature;
        try
        {
            if (key instanceof NodeStorage)
            {
                final NodeStorage tempStorage = (NodeStorage) value;
                //signature = TOMUtil.signMessage(keyLoader.loadPrivateKey(), tempStorage.getBytes());
                //tempStorage.addProperty("signature" + idClient, new String(signature, "UTF-8"));
                access.applyUpdate((NodeStorage) key, tempStorage, snapshotId);
            }
            else if (value instanceof RelationshipStorage)
            {
                final RelationshipStorage tempStorage = (RelationshipStorage) value;
                //signature = TOMUtil.signMessage(keyLoader.loadPrivateKey(), tempStorage.getBytes());
                //tempStorage.addProperty("signature" + idClient, new String(signature, "UTF-8"));
                access.applyUpdate((RelationshipStorage) key, tempStorage, snapshotId);
            }
            else
            {
                Log.getLogger().warn("Trying to update incorrect type in the database.");
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().warn("Unable to sign nodeStorage ", e);
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
    public boolean equals(Object e)
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
