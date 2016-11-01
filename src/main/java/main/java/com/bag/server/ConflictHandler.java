package main.java.com.bag.server;

import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Receives read and write sets and checks them for conflicts
 */
public class ConflictHandler
{
    //todo check hash (in database) , check timestamp, check past read and writeSets.

    private boolean checkForConflict(HashMap<Long, ArrayList<NodeStorage>> writeSetNode,
            HashMap<Long, ArrayList<NodeStorage>> writeSetRelationship,
            ArrayList<NodeStorage> readSetNode,
            ArrayList<RelationshipStorage> readSetRelationship,
            long snapshotId)
    {
        return isUpToDate(writeSetNode, writeSetRelationship, readSetNode, readSetRelationship, snapshotId) && isCorrect(readSetNode, readSetRelationship);
    }

    private boolean isUpToDate(HashMap<Long, ArrayList<NodeStorage>> writeSetNode,
            HashMap<Long, ArrayList<NodeStorage>> writeSetRelationship,
            ArrayList<NodeStorage> readSetNode,
            ArrayList<RelationshipStorage> readSetRelationship, long snapshotId)
    {

        return !writeSetNode.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> Collections.unmodifiableList(writeSetNode.get(id)).retainAll(readSetNode))
                && !writeSetRelationship.keySet().stream().filter(id -> id > snapshotId).anyMatch(id -> Collections.unmodifiableList(writeSetRelationship.get(id)).retainAll(readSetRelationship));
    }

    private boolean isCorrect(ArrayList<NodeStorage> readSetNode, ArrayList<RelationshipStorage> readSetRelationship)
    {
        //todo have to read from database and get hash
        return true;
    }


}
