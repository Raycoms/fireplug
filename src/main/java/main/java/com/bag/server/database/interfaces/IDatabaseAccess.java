package main.java.com.bag.server.database.interfaces;


import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.util.List;
import java.util.Map;

/**
 * Abstract class with required methods for all graph databases.
 */
public abstract interface IDatabaseAccess
{
    /**
     * Method used to start the graph database service.
     * @param id change foldername depending on the id.
     */
    public void start(int id);

    /**
     * Method used to terminate the graph database service.
     */
    public void terminate();

    /**
     * Method used to check if the hashes inside a readSet are correct.
     */
    public boolean equalHash(List readSet);

    /**
     * Executes a transaction on the database. Creating, updating and deleting data.
     * @param createSetNode the createSet of the nodes.
     * @param createSetRelationship the createSet of the relationships.
     * @param updateSetNode the updateSet of the nodes.
     * @param updateSetRelationship the updateSet of the relationships.
     * @param deleteSetNode the deleteSet of the nodes.
     * @param deleteSetRelationship the deleteSet of the relationships.
     */
    public void execute(List<NodeStorage> createSetNode, List<RelationshipStorage> createSetRelationship,
            Map<NodeStorage, NodeStorage> updateSetNode, Map<RelationshipStorage, RelationshipStorage> updateSetRelationship,
            List<NodeStorage> deleteSetNode, List<RelationshipStorage> deleteSetRelationship);

}
