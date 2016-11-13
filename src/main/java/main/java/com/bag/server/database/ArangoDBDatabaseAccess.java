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
    public String getType()
    {
        return Constants.ARANGODB;
    }

    @Override
    public void execute(
            final List<NodeStorage> createSetNode,
            final List<RelationshipStorage> createSetRelationship,
            final Map<NodeStorage, NodeStorage> updateSetNode,
            final Map<RelationshipStorage, RelationshipStorage> updateSetRelationship,
            final List<NodeStorage> deleteSetNode,
            final List<RelationshipStorage> deleteSetRelationship,
            final long snapshotId)
    {

    }
}
