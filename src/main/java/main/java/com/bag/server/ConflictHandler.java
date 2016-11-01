package main.java.com.bag.server;

import main.java.com.bag.server.database.Neo4jDatabaseAccess;
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

    protected static boolean checkForConflict(Map<Long, List<NodeStorage>> writeSetNode,
            Map<Long, List<RelationshipStorage>> writeSetRelationship,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship,
            long snapshotId)
    {
        return isUpToDate(writeSetNode, writeSetRelationship, readSetNode, readSetRelationship, snapshotId) && isCorrect(readSetNode, readSetRelationship);
    }

    private static boolean isUpToDate(Map<Long, List<NodeStorage>> writeSetNode,
            Map<Long, List<RelationshipStorage>> writeSetRelationship,
            List<NodeStorage> readSetNode,
            List<RelationshipStorage> readSetRelationship, long snapshotId)
    {

        return !writeSetNode.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> Collections.unmodifiableList(writeSetNode.get(id)).retainAll(readSetNode))
                && !writeSetRelationship.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> Collections.unmodifiableList(writeSetRelationship.get(id)).retainAll(readSetRelationship));
    }

    private static boolean isCorrect(List<NodeStorage> readSetNode, List<RelationshipStorage> readSetRelationship)
    {
        Neo4jDatabaseAccess neo4j = new Neo4jDatabaseAccess();
        return neo4j.equalHash(readSetNode) && neo4j.equalHash(readSetRelationship);
    }
}
