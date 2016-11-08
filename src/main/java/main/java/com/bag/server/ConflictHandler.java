package main.java.com.bag.server;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

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
     * @param writeSetNode the node writeSet.
     * @param writeSetRelationship the relationship writeSet.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if no conflict has been found.
     */
    protected static boolean checkForConflict(Map<Long, List<NodeStorage>> writeSetNode,
            Map<Long, List<RelationshipStorage>> writeSetRelationship,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship,
            long snapshotId, IDatabaseAccess access)
    {
        return isUpToDate(writeSetNode, writeSetRelationship, readSetNode, readSetRelationship, snapshotId) && isCorrect(readSetNode, readSetRelationship, access);
    }

    /**
     * Checks if no changes have been made since the start of the transaction.
     * @param writeSetNode the node writeSet.
     * @param writeSetRelationship the relationship writeSet.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if data is up to date.
     */
    private static boolean isUpToDate(Map<Long, List<NodeStorage>> writeSetNode,
            Map<Long, List<RelationshipStorage>> writeSetRelationship,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship, long snapshotId)
    {

        return !writeSetNode.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> Collections.unmodifiableList(writeSetNode.get(id)).retainAll(readSetNode))
                && !writeSetRelationship.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSetRelationship.get(id)).retainAll(readSetRelationship));
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
