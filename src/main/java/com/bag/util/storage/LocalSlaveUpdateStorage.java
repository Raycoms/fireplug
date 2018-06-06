package com.bag.util.storage;

import com.bag.operations.IOperation;

import java.util.List;

/**
 * Storage to describe a transaction.
 */
public class LocalSlaveUpdateStorage
{
    /**
     * The local write set.
     */
    private final List<IOperation> localWriteSet;

    /**
     * The read set for the nodes.
     */
    private final List<NodeStorage> readSetNode;

    /**
     * The read set for the relationships.
     */
    private final List<RelationshipStorage> readsSetRelationship;

    /**
     * The snapshot id.
     */
    private final long snapShotId;

    /**
     * Constructor for the storage.
     * @param localWriteSet the write set.
     * @param readSetNode node read set.
     * @param readsSetRelationship rs read set.
     * @param snapShotId the snapshot id.
     */
    public LocalSlaveUpdateStorage(
            final List<IOperation> localWriteSet,
            final List<NodeStorage> readSetNode,
            final List<RelationshipStorage> readsSetRelationship,
            final long snapShotId)
    {

        this.localWriteSet = localWriteSet;
        this.readSetNode = readSetNode;
        this.readsSetRelationship = readsSetRelationship;
        this.snapShotId = snapShotId;
    }

    /**
     * Getter for the write set.
     * @return a copy.
     */
    public List<IOperation> getLocalWriteSet()
    {
        return localWriteSet;
    }

    /**
     * Getter for the node read set.
     * @return a copy.
     */
    public List<NodeStorage> getReadSetNode()
    {
        return readSetNode;
    }

    /**
     * Getter for the rs read set.
     * @return a copy.
     */
    public List<RelationshipStorage> getReadsSetRelationship()
    {
        return readsSetRelationship;
    }

    /**
     * Getter for the snapshot id.
     * @return the id.
     */
    public long getSnapShotId()
    {
        return snapShotId;
    }
}
