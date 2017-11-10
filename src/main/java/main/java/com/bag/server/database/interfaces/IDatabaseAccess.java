package main.java.com.bag.server.database.interfaces;


import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.List;

/**
 * Abstract class with required methods for all graph databases.
 */
public interface IDatabaseAccess
{
    /**
     * Method used to start the graph database service.
     */
    void start();

    /**
     * Method used to terminate the graph database service.
     */
    void terminate();

    /**
     * Method used to check if the hashes inside a readSet are correct.
     */
    default boolean equalHash(final List readSet)
    {
        if(readSet.isEmpty())
        {
            return true;
        }

        if(readSet.get(0) instanceof NodeStorage)
        {
            equalHashNode(readSet);
        }
        else if(readSet.get(0) instanceof RelationshipStorage)
        {
            equalHashRelationship(readSet);
        }

        return true;
    }

    /**
     * Checks if the hash of a list of relationships matches the relationship in the database.
     * @param readSet the set of relationships
     * @return true if all are correct.
     */
    default boolean equalHashRelationship(final List readSet)
    {
        for(Object storage: readSet)
        {
            if(storage instanceof RelationshipStorage)
            {
                RelationshipStorage relationshipStorage = (RelationshipStorage) storage;

                if(!compareRelationship(relationshipStorage))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks if the hash of a node is equal to the hash in the database.
     * @param readSet the readSet of nodes which should be compared.
     * @return true if all nodes are equal.
     */
    default boolean equalHashNode(final List readSet)
    {
        for(Object storage: readSet)
        {
            if(storage instanceof NodeStorage)
            {
                NodeStorage nodeStorage = (NodeStorage) storage;

                if(!compareNode(nodeStorage))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares the relationshipStorage with it's hash from the database.
     * @param storage the storage to compare.
     * @return true if it matches.
     */
    boolean compareRelationship(RelationshipStorage storage);

    /**
     * Compares the node with it's hash from the database.
     * @param storage the storage to compare.
     * @return true if it matches.
     */
    boolean compareNode(NodeStorage storage);

    /**
     * Applies a node update to the database.
     * @return true if successful.
     */
    boolean applyUpdate(NodeStorage key, NodeStorage value, long snapshotId);

    /**
     * Applies a node create to the database.
     * @return true if successful.
     */
    boolean applyCreate(NodeStorage storage, long snapshotId);

    /**
     * Applies a node delete to the database.
     * @return true if successful.
     */
    boolean applyDelete(NodeStorage storage, long snapshotId);

    /**
     * Applies a node update to the database.
     * @return true if successful.
     */
    boolean applyUpdate(RelationshipStorage key, RelationshipStorage value, long snapshotId);

    /**
     * Applies a node create to the database.
     * @return true if successful.
     */
    boolean applyCreate(RelationshipStorage storage, long snapshotId);

    /**
     * Applies a node delete to the database.
     * @return true if successful.
     */
    boolean applyDelete(RelationshipStorage storage, long snapshotId);

    /**
     * Method to read an object from the database.
     * @param identifier identifier of the object.
     * @param localSnapshotId snapshotId.
     * @return list of objects.
     */
    List<Object> readObject(Object identifier, long localSnapshotId) throws OutDatedDataException;

    /**
     * Checks if this db should try to check requests with this sequence number.
     * @param sequence the sequence number.
     * @return true if so.
     */
    boolean shouldFollow(int sequence);
}
