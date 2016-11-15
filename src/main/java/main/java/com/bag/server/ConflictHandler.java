package main.java.com.bag.server;

import main.java.com.bag.operations.Operation;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.*;

/**
 * Receives read and write sets and checks them for conflicts
 */
public class ConflictHandler
{
    /**
     * Private standard constructor to hide the implicit one.
     */
    private ConflictHandler()
    {
        /*
         * Intentionally left empty.
         */
    }

    /**
     * Checks for conflicts between read and writeSets.
     * @param writeSet the node and relationship writeSet.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if no conflict has been found.
     */
    protected static boolean checkForConflict(Map<Long, List<Operation>> writeSet,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship,
            long snapshotId, IDatabaseAccess access)
    {
        return isUpToDate(writeSet, readSetNode, readSetRelationship, snapshotId) && isCorrect(readSetNode, readSetRelationship, access);
    }

    /**
     * Checks if no changes have been made since the start of the transaction.
     * @param writeSet the node and relationship writeSet.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if data is up to date.
     */
    private static boolean isUpToDate(Map<Long, List<Operation>> writeSet,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship, long snapshotId)
    {

        return !writeSet.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSet.get(id)).retainAll(readSetNode))
                && !writeSet.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSet.get(id)).retainAll(readSetRelationship));
    }

    /**
     * Checks if readData matches with data in database.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @return true if correct.
     */
    private static boolean isCorrect(List<NodeStorage> readSetNode, List<RelationshipStorage> readSetRelationship, IDatabaseAccess access)
    {
        return access.equalHash(readSetNode) && access.equalHash(readSetRelationship);
    }
}
