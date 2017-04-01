package main.java.com.bag.operations;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.Serializable;

/**
 * Update command which may be sent to the database.
 */
public class UpdateOperation<S extends Serializable> implements Operation, Serializable
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
    public void apply(final IDatabaseAccess access, long snapshotId)
    {
        if(key instanceof NodeStorage && value instanceof NodeStorage)
        {
            access.applyUpdate((NodeStorage) key,(NodeStorage) value, snapshotId);
        }
        else if(key instanceof RelationshipStorage && value instanceof RelationshipStorage)
        {
            access.applyUpdate((RelationshipStorage) key,(RelationshipStorage) value, snapshotId);
        }
        else
        {
            Log.getLogger().warn("Can't update Node with Relationship or vice versa.");
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
}
