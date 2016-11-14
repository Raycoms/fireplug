package main.java.com.bag.server.database;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.util.List;
import java.util.Map;

/**
 * Database access for the arangoDB database.
 */
public class ArangoDBDatabaseAccess implements IDatabaseAccess
{
    private final int id;
    public ArangoDBDatabaseAccess(int id)
    {
        this.id = id;
    }

    public void start()
    {

    }

    public void terminate()
    {

    }

    @Override
    public boolean equalHash(final List readSet)
    {
        return false;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        return false;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        return false;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        return false;
    }
}
