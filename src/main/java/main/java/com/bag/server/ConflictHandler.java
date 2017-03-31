package main.java.com.bag.server;

import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
     * @param globalWriteSet the node and relationship global writeSet.
     * @param localWriteSet the node and relationship write set of the transaction.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if no conflict has been found.
     */
    protected static boolean checkForConflict(Map<Long, List<Operation>> globalWriteSet, List<Operation> localWriteSet,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship,
            long snapshotId, IDatabaseAccess access)
    {
        return isUpToDate(globalWriteSet, localWriteSet, readSetNode, readSetRelationship, snapshotId) && isCorrect(readSetNode, readSetRelationship, access);
    }

    /**
     * Checks if no changes have been made since the start of the transaction.
     * @param writeSet the node and relationship writeSet.
     * @param localWriteSet the node and relationship writeSet of the transaction.
     * @param readSetNode the node readSet.
     * @param readSetRelationship the relationship readSet
     * @param snapshotId the snapShotId of the transaction.
     * @return true if data is up to date.
     */
    private static boolean isUpToDate(Map<Long, List<Operation>> writeSet, List<Operation> localWriteSet,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship, long snapshotId)
    {
        final boolean writeSetNodeWithRead = readSetNode.isEmpty() || !writeSet.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSet.get(id)).retainAll(readSetNode));
        final boolean writeSetRSWithRead = readSetRelationship.isEmpty() || !writeSet.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSet.get(id)).retainAll(readSetRelationship));

        final List<Operation> tempList = localWriteSet.stream().filter(operation -> operation instanceof DeleteOperation || operation instanceof UpdateOperation)
                .collect(Collectors.toList());

        final boolean writeWithWrite = tempList.isEmpty() || !writeSet.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> new ArrayList<>(writeSet.get(id))
                .retainAll(tempList));

        return writeSetNodeWithRead && writeSetRSWithRead && writeWithWrite;
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
