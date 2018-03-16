package main.java.com.bag.exceptions;

import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

import static main.java.com.bag.util.Constants.TAG_PRE;
import static main.java.com.bag.util.Constants.TAG_SNAPSHOT_ID;

/**
 * Exception thrown when outdated data is found on a read.
 */
public class OutDatedDataException extends Exception
{
    public final static int IGNORE_SNAPSHOT = -666;

    /**
     * Standard Constructor.
     */
    public OutDatedDataException()
    {
        super();
    }

    /**
     * Constructor with the simple message.
     * @param message incoming message.
     */
    public OutDatedDataException(final String message) { super(message); }

    /**
     * Constructor with the message and cause.
     * @param message the message.
     * @param cause the cause.
     */
    public OutDatedDataException(final String message, final Throwable cause) { super(message, cause); }

    /**
     * Constructor with only the cause.
     * @param cause the cause.
     */
    public OutDatedDataException(final Throwable cause) { super(cause); }

    /**
     * Checks if an input is a valid snapshotId for the transaction.
     * @param input the input to check.
     * @param snapshotId the real snapshotId.
     * @throws OutDatedDataException exception to throw if not valid.
     */
    public static void checkSnapshotId(final Object input, final long snapshotId) throws OutDatedDataException
    {
        long tempSnapshotId = 0;

        if(input instanceof String)
        {
            tempSnapshotId = Long.valueOf((String) input);
        }
        else if(input instanceof Long)
        {
            tempSnapshotId = (long) input;
        }

        if(tempSnapshotId > snapshotId && snapshotId != IGNORE_SNAPSHOT)
        {
            throw new OutDatedDataException("Requested node or relationship has been updated by the database since the start of this transaction");
        }
    }

    /**
     * Checks if an input is a valid snapshotId for the transaction.
     * @param input the input to check.
     * @param snapshotId the real snapshotId.
     * @param storage the storage object.
     * @throws OutDatedDataException exception to throw if not valid.
     */
    public static NodeStorage getCorrectNodeStorage(final Object input, final long snapshotId, final NodeStorage storage)
    {
        long tempSnapshotId = 0;

        if(input instanceof String)
        {
            tempSnapshotId = Long.valueOf((String) input);
        }
        else if(input instanceof Long)
        {
            tempSnapshotId = (long) input;
        }

        NodeStorage tempStorage = storage;
        if(tempSnapshotId > snapshotId && snapshotId != IGNORE_SNAPSHOT)
        {
            final Object sId = storage.getProperties().get(TAG_SNAPSHOT_ID);
            while(sId instanceof Long && (long) sId < tempSnapshotId && storage.getProperty(TAG_PRE) instanceof NodeProxy)
            {
                final NodeProxy n = (NodeProxy) storage.getProperty(TAG_PRE);
                tempStorage = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
            }
        }
        return tempStorage;
    }

    /**
     * Checks if an input is a valid snapshotId for the transaction.
     * @param input the input to check.
     * @param snapshotId the real snapshotId.
     * @param storage the storage object.
     * @throws OutDatedDataException exception to throw if not valid.
     */
    public static RelationshipStorage getCorrectRSStorage(final Object input, final long snapshotId, final RelationshipStorage storage)
    {
        long tempSnapshotId = 0;

        if(input instanceof String)
        {
            tempSnapshotId = Long.valueOf((String) input);
        }
        else if(input instanceof Long)
        {
            tempSnapshotId = (long) input;
        }

        RelationshipStorage tempStorage = storage;
        if(tempSnapshotId > snapshotId && snapshotId != IGNORE_SNAPSHOT)
        {
            final Object sId = storage.getProperties().get(TAG_SNAPSHOT_ID);
            while(sId instanceof Long && (long) sId < tempSnapshotId && storage.getProperty(TAG_PRE) instanceof RelationshipProxy)
            {
                final RelationshipProxy r = (RelationshipProxy) storage.getProperty(TAG_PRE);
                final NodeStorage start = new NodeStorage(r.getStartNode().getLabels().iterator().next().name(), r.getStartNode().getAllProperties());
                final NodeStorage end = new NodeStorage(r.getEndNode().getLabels().iterator().next().name(), r.getEndNode().getAllProperties());

                tempStorage = new RelationshipStorage(r.getType().name(), r.getAllProperties(), start, end);
            }
        }
        return tempStorage;
    }
}
